'''
Created on Sep 24, 2019

@author: liang.li
'''
from random import sample
from threading import RLock
from uuid import uuid1
from ..base import Object
from .connection import ConnectionPool

class ConnectionManager(Object):
    __pools_reading = {}
    __pools_writing = {}
    __pools_lock = RLock()
    
    class Context:
        __slots__ = ('__connection',)
        
        def __init__(self, connection):
            self.__connection = connection
        def __enter__(self):
            return self.__connection
        def __exit__(self, *_):
            if self.__connection:
                self.__connection.close()
                self.__connection = None
    
    @classmethod
    def get_pool(cls, name=None, reading=False):
        with cls.__pools_lock:
            pools = (cls.__pools_reading if cls.__pools_reading else cls.__pools_writing) if reading else cls.__pools_writing
            if pools:
                if name is None:
                    return pools[sample(pools.keys(), 1)[0]]
                name = str(name)
                if name in pools:
                    return pools[name]
        return None
    @classmethod
    def set_pool(cls, name, maxsize=8, reading=False, timeout=30, recycle=18000, url=None):
        if name is None:
            name = str(uuid1()).replace('-', '')
        elif not isinstance(name, str):
            name = str(name)
        pools = cls.__pools_reading if reading else cls.__pools_writing
        with cls.__pools_lock:
            pools[name] = ConnectionPool(url, maxsize, timeout=timeout, recycle=recycle)
    @classmethod
    def get_connection(cls, name=None, reading=False):
        pool = cls.get_pool(name, reading)
        if pool is not None:
            try:
                return pool.get_connection()
            except:pass
        return None
    @classmethod
    def connect(cls, name=None, reading=False):
        pool = cls.get_pool(name, reading)
        if pool is None:
            connection = None
        else:
            try:
                connection = pool.get_connection()
            except:
                connection = None
        return cls.Context(connection)
    
    def __init__(self):raise
