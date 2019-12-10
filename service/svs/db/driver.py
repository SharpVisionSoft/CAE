'''
Created on Sep 23, 2019

@author: liang.li
'''
from threading import RLock
from ..base import ObjectMeta, Object
from ..util.url import URL
from .query import Filter

class DriverMeta(ObjectMeta):
    __driver_classes = {}
    
    def __new__(cls, name, bases, attrs):
        if name == 'Driver':
            return ObjectMeta.__new__(cls, name, bases, attrs)
        try:
            m, fns = attrs['__driver_module__']
            try:
                attrs['__driver_module__'] = __import__(m, fromlist=[x.strip() for x in fns.split(',')])
                clazz = ObjectMeta.__new__(cls, name, bases, attrs)
                cls.__driver_classes[m] = clazz
            except:
                clazz = ObjectMeta.__new__(cls, name, bases, attrs)
            finally:
                return clazz
        except:pass
        error = "class '%s' should has an attribute '__driver_module__' likes '('module name', 'fn1, fn2')'" % name
        raise AttributeError(error)
    @classmethod
    def get_driver_class(cls, name):
        return cls.__driver_classes[name] if name in cls.__driver_classes else None

class Driver(Object, metaclass=DriverMeta):
    __drivers = {}
    __drivers_lock = RLock()
    
    @classmethod
    def get(cls, name):
        with cls.__drivers_lock:
            if name in cls.__drivers:
                return cls.__drivers[name]
            clazz = cls.get_driver_class(name)
            if clazz is not None:
                driver = clazz()
                cls.__drivers[name] = driver
                return driver
        error = "Driver '%s' is not implemented" % name
        raise NotImplementedError(error)
    
    def connect(self, **kwargs):pass
    def lastrowid(self, cursor):pass
    def stored_results(self, cursor):pass
    def sql_select(self, tablename, columns=None, query_filter=None, page=0, length=10):pass
    def sql_insert(self, tablename, columns, auto_increment_columns):pass
    def sql_update(self, tablename, columns, query_filter=None):pass
    def sql_delete(self, tablename, query_filter=None):pass

class DriverMySQLConnector(Driver):
    __driver_module__ = ('mysql.connector', 'connect')
    
    @classmethod
    def __parse_connection_params(cls, url):
        return {
            'user': url.username,
            'password': url.password,
            'host': url.hostname,
            'port': 3306 if url.port is None else url.port,
            'database': url.path[1:]
        }
    
    def connect(self, **kwargs):
        if 'url' in kwargs:
            url = kwargs['url']
            if isinstance(url, URL):
                kwargs = self.__parse_connection_params(url)
            elif isinstance(url, str):
                kwargs = self.__parse_connection_params(URL(url))
            else:
                del kwargs['url']
        return self.__driver_module__.connect(**kwargs)
    def lastrowid(self, cursor):
        return (cursor.lastrowid,)
    def stored_results(self, cursor):
        return [[dict(zip(list(zip(*r._description))[0], x)) for x in r.fetchall()] for r in cursor.stored_results()]
    def sql_select(self, tablename, columns=None, query_filter=None, page=0, length=10):
        columns = columns if isinstance(columns, str) and columns else '*'
        if isinstance(query_filter, Filter):
            sql, params = query_filter.to_sql()
            if page > 0:
                sql_select = 'SELECT {} FROM {} WHERE {} LIMIT {},{}'.format(columns, tablename, sql, (page - 1) * length, length)
                sql_count = 'SELECT COUNT(0) FROM {} WHERE {}'.format(tablename, sql)
            else:
                sql_select = 'SELECT {} FROM {} WHERE {}'.format(columns, tablename, sql)
                sql_count = None
            return (sql_select, params, sql_count)
        if page > 0:
            sql_select = 'SELECT {} FROM {} LIMIT {},{}'.format(columns, tablename, (page - 1) * length, length)
            sql_count = 'SELECT COUNT(0) FROM {}'.format(tablename)
        else:
            sql_select = 'SELECT {} FROM {}'.format(columns, tablename)
            sql_count = None
        return (sql_select, (), sql_count)
    def sql_insert(self, tablename, columns, auto_increment_columns):
        if isinstance(auto_increment_columns, str):
            columns = [x.strip() for x in columns.split(',')]
            for x in auto_increment_columns.split(','):
                x = x.strip()
                if x in columns:
                    columns.remove(x)
            columns = ','.join(columns)
        return 'INSERT INTO {} ({}) VALUES ({})'.format(tablename, columns, ('%s,' * len(columns.split(',')))[:-1])
    def sql_update(self, tablename, columns, query_filter=None):
        if isinstance(query_filter, Filter):
            sql, params = query_filter.to_sql()
            return ('UPDATE {} SET {} WHERE {}'.format(tablename, ','.join('{}=%s'.format(col) for col in columns.split(',')), sql), params)
        return ('UPDATE {} SET {}'.format(tablename, ','.join('{}=%s'.format(col) for col in columns.split(','))), ())
    def sql_delete(self, tablename, query_filter=None):
        if isinstance(query_filter, Filter):
            sql, params = query_filter.to_sql()
            return ('DELETE FROM {} WHERE {}'.format(tablename, sql), params)
        return ('DELETE FROM {}'.format(tablename), ())

class DriverFDB(Driver):
    __driver_module__ = ('fdb', 'connect')
        
    @classmethod
    def __parse_connection_params(cls, url):
        return {
            'user': url.username,
            'password': url.password,
            'host': url.hostname,
            'port': 3050 if url.port is None else url.port,
            'database': url.path[1:],
            'charset': 'utf8'
        }
    
    def connect(self, **kwargs):
        if 'url' in kwargs:
            url = kwargs['url']
            if isinstance(url, URL):
                kwargs = self.__parse_connection_params(url)
            elif isinstance(url, str):
                kwargs = self.__parse_connection_params(URL(url))
            else:
                del kwargs['url']
        return self.__driver_module__.connect(**kwargs)
    def lastrowid(self, cursor):
        return cursor.fetchall()[0]
    def stored_results(self, cursor):
        columns = tuple(x.lower() for x in list(zip(*cursor.description))[0])
        return [[dict(zip(columns, x)) for x in cursor]]
    def sql_select(self, tablename, columns=None, query_filter=None, page=0, length=10):
        columns = columns if isinstance(columns, str) and columns else '*'
        if isinstance(query_filter, Filter):
            sql, params = query_filter.to_sql('?')
            if page > 0:
                sql_select = 'SELECT FIRST {} SKIP {} {} FROM {} WHERE {}'.format(length, (page - 1) * length, columns, tablename, sql)
                sql_count = 'SELECT COUNT(0) FROM {} WHERE {}'.format(tablename, sql)
            else:
                sql_select = 'SELECT {} FROM {} WHERE {}'.format(columns, tablename, sql)
                sql_count = None
            return (sql_select, params, sql_count)
        if page > 0:
            sql_select = 'SELECT FIRST {} SKIP {} {} FROM {}'.format(length, (page - 1) * length, columns, tablename)
            sql_count = 'SELECT COUNT(0) FROM {}'.format(tablename)
        else:
            sql_select = 'SELECT {} FROM {}'.format(columns, tablename)
            sql_count = None
        return (sql_select, (), sql_count)
    def sql_insert(self, tablename, columns, auto_increment_columns):
        if isinstance(auto_increment_columns, str):
            columns = [x.strip() for x in columns.split(',')]
            for x in auto_increment_columns.split(','):
                x = x.strip()
                if x in columns:
                    columns.remove(x)
            return 'INSERT INTO {} ({}) VALUES ({}) RETURNING {}'.format(tablename, ','.join(columns), ('?,' * len(columns))[:-1], auto_increment_columns)
        return 'INSERT INTO {} ({}) VALUES ({})'.format(tablename, columns, ('?,' * len(columns.split(',')))[:-1])
    def sql_update(self, tablename, columns, query_filter=None):
        if isinstance(query_filter, Filter):
            sql, params = query_filter.to_sql('?')
            return ('UPDATE {} SET {} WHERE {}'.format(tablename, ','.join('{}=?'.format(col) for col in columns.split(',')), sql), params)
        return ('UPDATE {} SET {}'.format(tablename, ','.join('{}=?'.format(col) for col in columns.split(','))), ())
    def sql_delete(self, tablename, query_filter=None):
        if isinstance(query_filter, Filter):
            sql, params = query_filter.to_sql('?')
            return ('DELETE FROM {} WHERE {}'.format(tablename, sql), params)
        return ('DELETE FROM {}'.format(tablename), ())
