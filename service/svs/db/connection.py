'''
Created on Sep 24, 2019
@author: Liang.Li
'''
from collections import deque
from threading import RLock, Condition
from time import time as time_now
from ..base import Object
from ..util.url import URL
from .driver import Driver
from .dao import DAOMeta

class Connection(Object):
    __slots__ = ('__raw_connection', '__last_active_time', '__url', '__pool')
    
    def __init__(self, url, pool):
        self.__raw_connection = Driver.get(url.scheme).connect(url=url)
        self.__last_active_time = time_now()
        self.__url = url
        self.__pool = pool
    def __del__(self):
        try:
            self.__raw_connection.close()
        except:pass
        self.__raw_connection = None
        self.__last_active_time = None
        self.__url = None
        self.__pool = None
    def refresh(self, timeout):
        if timeout > 0 and time_now() - self.__last_active_time >= timeout:
            try:
                self.__raw_connection.close()
            except:pass
            self.__raw_connection = self.__driver.connect(url=self.__url)
            self.__last_active_time = time_now()
    def commit(self):
        self.__raw_connection.commit()
    def rollback(self):
        self.__raw_connection.rollback()
    def close(self):
        try:
            self.__raw_connection.rollback();
        except:pass
        self.__last_active_time = time_now()
        self.__pool._do_return_connection(self)
    def select(self, tablename, columns=None, query_filter=None, page=0, length=10, clazz=None):
        sql_select, params, sql_count = Driver.get(self.__url.scheme).sql_select(tablename, columns, query_filter, page, length)
        cursor = self.__raw_connection.cursor()
        try:
            if sql_count is None:
                count = None
            else:
                cursor.execute(sql_count, params)
                count = cursor.fetchall()[0][0]
            cursor.execute(sql_select, params)
            columns = tuple(x.lower() for x in list(zip(*cursor.description))[0])
            return (
                True,
                [clazz(**dict(zip(columns, x))) for x in cursor.fetchall()] if isinstance(clazz, DAOMeta) else [dict(zip(columns, x)) for x in cursor.fetchall()],
                cursor.rowcount if count is None else count
            )
        finally:
            cursor.close()
    def insert(self, tablename, columns, data, auto_increment_columns):
        if isinstance(data, (list, tuple)) and data:
            driver = Driver.get(self.__url.scheme)
            sql = driver.sql_insert(tablename, columns, auto_increment_columns)
            cursor = self.__raw_connection.cursor()
            try:
                if len(data) > 1:
                    cursor.executemany(sql, data)
                else:
                    cursor.execute(sql, data[0])
                lastrowid = driver.lastrowid(cursor)
                return (True, lastrowid)
            finally:
                cursor.close()
        return (False, None)
    def update(self, tablename, columns, data, query_filter=None):
        sql, params = Driver.get(self.__url.scheme).sql_update(tablename, columns, query_filter)
        cursor = self.__raw_connection.cursor()
        try:
            cursor.execute(sql, tuple(data) + params)
        finally:
            cursor.close()
    def delete(self, tablename, query_filter=None):
        sql, params = Driver.get(self.__url.scheme).sql_delete(tablename, query_filter)
        cursor = self.__raw_connection.cursor()
        try:
            cursor.execute(sql, params)
        finally:
            cursor.close()
    def callproc(self, name, *args):
        cursor = self.__raw_connection.cursor()
        try:
            params = cursor.callproc(name, args)
            r = Driver.get(self.__url.scheme).stored_results(cursor)
            return (True, r, len(r), params)
        except Exception as err:
            raise err
        finally:
            cursor.close()

class ConnectionPool(Object):
    __slots__ = ('__url', '__maxsize', '__queue', '__lock', '__not_empty', '__not_full', '__max_overflow', '__overflow', '__overflow_lock', '__timeout', '__recycle')
           
    def __init__(self, url, maxsize=8, overflow=0, timeout=30, recycle=18000):#5 * 60 * 60s
        url = URL(url)
        connection = Connection(url, self)
        self.__url = url
        self.__maxsize = maxsize if isinstance(maxsize, int) and maxsize > 0 else 8
        self.__queue = deque()
        self.__lock = RLock()
        self.__not_empty = Condition(self.__lock)
        self.__not_full = Condition(self.__lock)
        self.__overflow = 0 - maxsize
        self.__overflow_lock = RLock()
        self.__max_overflow = overflow
        self.__timeout = timeout
        self.__recycle = recycle
        self.__put(connection, overflow > 0 and self.__overflow >= overflow, timeout)
    def __is_empty(self):
        return not self.__queue
    def __is_full(self):
        return self.__maxsize > 0 and len(self.__queue) == self.__maxsize
    def __put(self, connection, block=True, timeout=None):
        with self.__not_full:
            if not block:
                if self.__is_full():
                    raise 'Exception raised by Queue.put(block=0)/put_time_nowait().'
            elif timeout is None:
                while self.__is_full():
                    self.__not_full.wait()
            else:
                if timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                endtime = time_now() + timeout
                while self.__is_full():
                    remaining = endtime - time_now()
                    if remaining <= 0.0:
                        raise 'Exception raised by Queue.put(block=0)/put_nowait().'
                    self.__not_full.wait(remaining)
            self.__queue.append(connection)
            self.__not_empty.notify()
    def __get(self, block=True, timeout=None):
        with self.__not_empty:
            if not block:
                if self.__is_empty():
                    raise 'Exception raised by Queue.get(block=0)/get_nowait().'
            elif timeout is None:
                while self.__is_empty():
                    self.__not_empty.wait()
            else:
                if timeout < 0:
                    raise ValueError("'timeout' must be a positive number")
                endtime = time_now() + timeout
                while self.__is_empty():
                    remaining = endtime - time_now()
                    if remaining <= 0.0:
                        raise 'Exception raised by Queue.get(block=0)/get_nowait().'
                    self.__not_empty.wait(remaining)
            connection = self.__queue.pop()
            self.__not_full.notify()
        connection.refresh(self.__recycle)
        return connection
    def __inc_overflow(self):
        if self.__max_overflow <= 0:
            self.__overflow += 1
            return True
        with self.__overflow_lock:
            if self.__overflow < self.__max_overflow:
                self.__overflow += 1
                return True
        return False
    def __dec_overflow(self):
        if self.__max_overflow <= 0:
            self.__overflow -= 1
        else:
            with self.__overflow_lock:
                self.__overflow -= 1
        return True
    def _do_return_connection(self, connection):
        if isinstance(connection, Connection):
            self.__put(connection, self.__max_overflow > 0 and self.__overflow >= self.__max_overflow, self.__timeout)
    def get_connection(self):
        use_overflow = self.__max_overflow > 0
        wait = use_overflow and self.__overflow >= self.__max_overflow
        try:
            return self.__get(wait, self.__timeout)
        except:pass
        if use_overflow and self.__overflow >= self.__max_overflow:
            if not wait:
                return self.get_connection()
            else:
                raise TimeoutError
        if self.__inc_overflow():
            try:
                return Connection(self.__url, self)
            except Exception as err:
                self.__dec_overflow()
                raise err
        return self.get_connection()
