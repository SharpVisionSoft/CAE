'''
Created on Sep 25, 2019
@author: Liang.Li
'''
from ..base import Object
from .mgr import ConnectionManager

def select(tablename, columns=None, query_filter=None, page=0, length=10, dbname=None, clazz=None):
    '''
    tablename: str
    columns: comma separated str
    query_filter: db.query.Filter
    page: int
    length: int
    dbname: str
    clazz: DAO class
    '''
    with ConnectionManager.connect(dbname, reading=True) as c:
        if c is None:
            return (False, 'There has no database connection', None)
        try:
            return c.select(tablename, columns, query_filter, page, length, clazz)
        except Exception as e:
            return (False, str(e), None)
        finally:
            c.rollback()

def insert(tablename, columns, data, auto_increment_columns=None, dbname=None):
    '''
    tablename: str
    columns: comma separated str
    data: list or tuple
    auto_increment_columns: comma separated str
    dbname: str
    '''
    with ConnectionManager.connect(dbname) as c:
        if c is None:
            return (False, 'There has no database connection', None, None)
        if not (isinstance(auto_increment_columns, str) and auto_increment_columns):
            auto_increment_columns = 'id'
        try:

            r = c.insert(tablename, columns, data, auto_increment_columns)
            if r[0]:
                c.commit()
            return r
        except Exception as e:
            c.rollback()
            return (False, str(e))

def update(tablename, columns, data, query_filter=None, dbname=None):
    '''
    tablename: str
    columns: comma separated str
    data: list or tuple
    query_filter: db.query.Filter
    dbname: str
    '''
    with ConnectionManager.connect(dbname) as c:
        if c is None:
            return (False, 'There has no database connection')
        try:
            c.update(tablename, columns, data, query_filter)
            c.commit()
            return (True, None)
        except Exception as e:
            c.rollback()
            return (False, str(e))

def delete(tablename, query_filter=None, dbname=None):
    '''
    tablename: str
    query_filter: db.query.Filter
    dbname: str
    '''
    with ConnectionManager.connect(dbname) as c:
        if c is None:
            return (False, 'There has no database connection')
        try:
            c.delete(tablename, query_filter)
            c.commit()
            return (True, None)
        except Exception as e:
            c.rollback()
            return (False, str(e))

def callproc(name, reading=True, dbname=None, *args):
    '''
    name: str
    args: any variable parameters
    reading: bool
    dbname: str
    '''
    if reading:
        with ConnectionManager.connect(dbname, reading=True) as c:
            if c is None:
                return (False, 'There has no database connection', None, None)
            try:
                return c.callproc(name, *args)
            except Exception as e:
                return (False, str(e), None, None)
            finally:
                c.rollback()
    with ConnectionManager.connect(dbname) as c:
        if c is None:
            return (False, 'There has no database connection', None, None)
        try:
            r = c.callproc(name, *args)                
            c.commit()
            return r
        except Exception as e:
            c.rollback()
            return (False, str(e), None, None)

class Session(Object):
    __slots__ = ('__dbname', '__connection', '__dirty')
    __error = "The method 'begin' should be invoked first"
    
    def __init__(self, dbname=None):
        self.__dbname = dbname
        self.__connection = None
        self.__dirty = False
    def __del__(self):
        if self.__connection is not None:
            self.__end(to_rollback=self.__dirty)
    def __enter__(self):
        self.begin()
        return self
    def __exit__(self, *_):
        self.end()
    def __end(self, to_rollback=False):
        if self.__dirty:
            if to_rollback:
                self.__connection.rollback()
            else:
                self.__connection.commit()
        self.__connection.close()
        self.__connection = None
        self.__dirty = False
    def begin(self):
        if self.__connection is None:
            self.__connection = ConnectionManager.get_connection(self.__dbname)
            if self.__connection is None:
                raise 'There has no database connection'
    def end(self):
        if self.__connection is not None:
            self.__end()
    def select(self, tablename, columns=None, query_filter=None, page=0, length=10, clazz=None):
        if self.__connection is None:
            raise self.__error
        c = ConnectionManager.get_connection(self.__dbname, reading=True)
        if c is None:
            c = self.__connection
            try:
                return c.select(tablename, columns, query_filter, page, length, clazz)
            except Exception as e:
                self.__end(to_rollback=True)
                raise e
        else:
            try:
                return c.select(tablename, columns, query_filter, page, length, clazz)
            except Exception as e:
                self.__end(to_rollback=True)
                raise e
            finally:
                c.close()        
    def insert(self, tablename, columns, data, auto_increment_columns=None):
        if self.__connection is None:
            raise self.__error
        try:
            r = self.__connection.insert(tablename, columns, data, auto_increment_columns)
            self.__dirty = True
            return r
        except Exception as e:
            self.__end(to_rollback=True)
            raise e
    def update(self, tablename, columns, data, query_filter=None):
        if self.__connection is None:
            raise self.__error
        try:
            r = self.__connection.update(tablename, columns, data, query_filter)
            self.__dirty = True
            return r
        except Exception as e:
            self.__end(to_rollback=True)
            raise e
    def delete(self, tablename, query_filter=None):
        if self.__connection is None:
            raise self.__error
        try:
            r = self.__connection.delete(tablename, query_filter)
            self.__dirty = True
            return r
        except Exception as e:
            self.__end(to_rollback=True)
            raise e
    def callproc(self, name, *args, reading=True):
        if self.__connection is None:
            raise self.__error
        if reading:
            c = ConnectionManager.get_connection(self.__dbname, reading=True)
            if c is None:
                c = self.__connection
                try:
                    return c.callproc(name, *args)
                except Exception as e:
                    self.__end(to_rollback=True)
                    raise e
            else:
                try:
                    return c.callproc(name, *args)
                except Exception as e:
                    self.__end(to_rollback=True)
                    raise e
                finally:
                    c.close()
        else:
            try:
                r = self.__connection.callproc(name, *args)
                self.__dirty = True
                return r
            except Exception as e:
                self.__end(to_rollback=True)
                raise e
