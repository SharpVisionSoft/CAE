'''
Created on Sep 23, 2019

@author: liang.li
'''
from ..base import ObjectMeta, Entity

class DAOMeta(ObjectMeta):
    def __new__(cls, name, bases, attrs):
        if name != 'DAO' and '__tablename__' not in attrs:
            error = "class '%s' should has an attribute '__tablename__'" % name
            raise AttributeError(error)
        return ObjectMeta.__new__(cls, name, bases, attrs)

class DAO(Entity, metaclass=DAOMeta):
    
    def __init__(self, **kwargs):
        for k in kwargs:
            if hasattr(self.__class__, k):
                setattr(self, k, kwargs[k])
    def unzip(self):
        columns = []
        data = []
        for k in self.__predefined_public_attribute_names__:
            if hasattr(self, k):
                columns.append(k)
                data.append(getattr(self, k))
        return (self.__tablename__, ','.join(columns), data)
