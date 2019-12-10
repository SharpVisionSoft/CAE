'''
Created on Jun 20, 2019

@author: liang.li
'''
from json import dumps, loads, JSONEncoder

class ObjectMeta(type):
    @staticmethod
    def __predefined_public_attribute_names(bases, attrs):
        if '__slots__' in attrs:
            a = {n: None for n in attrs['__slots__'] if not n.startswith('_')}
        else:
            attrs['__slots__'] = ()
            a = {}
        for b in bases:
            if hasattr(b, '__predefined_public_attribute_names__'):
                for n in b.__predefined_public_attribute_names__:
                    a[n] = None
            elif hasattr(b, '__slots__'):
                for n in b.__slots__:
                    if not n.startswith('_'):
                        a[n] = None
            else:
                for n in b.__dict__:
                    if not (n.startswith('_') or hasattr(b.__dict__[n], '__call__')):
                        a[n] = None
        attrs['__predefined_public_attribute_names__'] = tuple(n for n in a)
        return attrs
    def __new__(cls, name, bases, attrs):
        return type.__new__(cls, name, bases, cls.__predefined_public_attribute_names(bases, attrs))

class Object(object, metaclass=ObjectMeta):
    def __name_value_pair_default(self):
        for k in self.__predefined_public_attribute_names__:
            if hasattr(self, k):
                yield (k, getattr(self, k))
        if hasattr(self, '__dict__'):
            for k, v in self.__dict__.items():
                if not (k.startswith('_') or hasattr(v, '__call__')):
                    yield (k, v)
    def __name_value_pair_names(self, names):
        for k in names:
            if not k.startswith('_') and hasattr(self, k):
                v = getattr(self, k)
                if not hasattr(v, '__call__'):
                    yield (k, v)
    def attributes(self, names=None):
        if isinstance(names, str):
            return {k: v for k, v in self.__name_value_pair_names((n.strip() for n in names.split(',')))}
        if isinstance(names, (list, tuple)):
            return {k: v for k, v in self.__name_value_pair_names(names)}
        return {k: v for k, v in self.__name_value_pair_default()}
    def __repr__(self):
        return str(self.attributes())

class Entity(Object):
    class __JSONEncoder(JSONEncoder):
        def default(self, o):
            if isinstance(o, Entity):
                return o.attributes()
            if isinstance(o, bytes):
                return o.decode()
            return super().default(o)
    
    @classmethod
    def stringify(cls, e):
        return dumps(e, cls=cls.__JSONEncoder)
    @classmethod
    def parse(cls, json, clazz=None):
        d = loads(json)
        if isinstance(clazz, type):
            o = clazz()
            for k in d:
                if hasattr(clazz, k):
                    setattr(o, k, d[k])
            return o
        return d
    
    def __contains__(self, name):
        return hasattr(self, name)
    def __len__(self):
        return len([k for k in self.__iter__()])
    def __getitem__(self, name):
        if hasattr(self, name):
            return getattr(self, name)
        raise AttributeError(name)
    def __setitem__(self, name, value):
        if hasattr(self.__class__, name):
            setattr(self, name, value)
        else:
            raise AttributeError(name)
    def __delitem__(self, name):
        if hasattr(self, name):
            delattr(self, name)
        else:
            raise AttributeError(name)
    def __iter__(self):
        for k in self.__predefined_public_attribute_names__:
            if hasattr(self, k):
                yield k
        if hasattr(self, '__dict__'):
            for k, v in self.__dict__.items():
                if not (k.startswith('_') or hasattr(v, '__call__')):
                    yield k
    def to_json(self, names=None):
        return self.stringify(self.attributes(names))
