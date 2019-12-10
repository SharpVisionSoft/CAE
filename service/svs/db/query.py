'''
Created on Sep 24, 2019
@author: Liang.Li
'''
from copy import deepcopy
from enum import Enum
from io import StringIO
from ..base import Object

class Filter(Object):
    __slots__ = ('__entries', '_symbol')
    
    class OP(Enum):
        EQUAL = '=%s'
        NOT_EQUAL = '!=%s'
        GREATER = '>%s'
        GREATER_OR_EQUAL = '>=%s'
        LESS = '<%s'
        LESS_OR_EQUAL = '<=%s'
        NULL = ' IS NULL'
        NOT_NULL = ' IS NOT NULL'
        LIKE = ' LIKE %s'
        BETWEEN = ' BETWEEN %s AND %s'
    
    class __Entry(Object):
        __slots__ = ('__symbol', '__name', '__op', '__value')
        
        def __init__(self, symbol, name, op, value):
            self.__symbol = symbol
            self.__name = name
            self.__op = op
            self.__value = value
        def to_sql(self):
            return (
                '{}{}{}'.format(self.__symbol, self.__name, self.__op.value),
                () if self.__op == Filter.OP.NULL or self.__op == Filter.OP.NOT_NULL else self.__value
            )
        def _name(self, name):
            self.__name = name;
        def _op(self, op):
            self.__op = op
        def _value(self, value):
            self.__value = value;
    
    def __init__(self, name=None, op=None, value=None):
        self.__entries = [self.__Entry('', name, op, value)] if isinstance(name, str) and isinstance(op, Filter.OP) else []
        self._symbol = ''
    def __add(self, symbol, name=None, op=None, value=None, query_filter=None):
        if isinstance(name, str) and isinstance(op, Filter.OP):
            if len(self.__entries) > 0:
                self.__entries.append(self.__Entry(symbol, name, op, value))
            else:
                self.__entries.append(self.__Entry('', name, op, value))
        if isinstance(query_filter, Filter):
            qf = deepcopy(query_filter)
            qf._symbol = symbol if len(self.__entries) > 0 else ''
            self.__entries.append(qf)
    def _name(self, name, index=0):
        self.__entries[index]._name(name)
    def _value(self, value, index=0):
        self.__entries[index]._value(value)
    def AND(self, name=None, op=None, value=None, query_filter=None):
        self.__add(' and ', name, op, value, query_filter)
        return self
    def OR(self, name=None, op=None, value=None, query_filter=None):
        self.__add(' or ', name, op, value, query_filter)
        return self
    def to_sql(self, placeholder=None):
        params = []
        with StringIO() as writer:
            for e in self.__entries:
                s, d = e.to_sql()
                writer.write('{}({})'.format(e._symbol, s) if isinstance(e, Filter) else s)
                if isinstance(d, (list, tuple)):
                    params.extend(d)
                else:
                    params.append(d)
            sql = writer.getvalue()
            if isinstance(placeholder, str):
                sql = sql.replace('%s', placeholder)
        return (sql, tuple(params))
