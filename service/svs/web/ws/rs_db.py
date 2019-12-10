'''
Created on Sep 25, 2019

@author: liang.li
'''
from http import HTTPStatus
from ...db.query import Filter
from ...db import accessor
from .rs import Resource

class DatabaseAccessResource(Resource):
    __slots__ = ('__black_names',)
    
    __converters = {'bool': bool, 'double': float, 'float': float, 'int': int, 'long': int}
    __default_query_string = {'page': '0', 'length': '10', 'columns': None, 'db': None, 'names': None, 'types': None}
    
    @classmethod
    def __cast_value(cls, v, i, l, types):
        converters = cls.__converters
        if i < l and types[i] in converters:
            try:
                return converters[types[i]](v)
            except:pass
        return v
    @classmethod
    def parse_query_filter(cls, values, names=None, types=None):
        if isinstance(names, str):
            values = values.split(',')
            names = names.split(',')
            qf = Filter()
            if isinstance(types, str):
                types = [x.strip().lower() for x in types.split(',')]
                l = len(types)
                c = cls.__cast_value
                for i in range(min(len(values), len(names))):
                    qf.AND(names[i], Filter.OP.EQUAL, c(values[i], i, l, types))
                return qf
            for i in range(min(len(values), len(names))):
                qf.AND(names[i], Filter.OP.EQUAL, values[i])
            return qf
        return Filter('id', Filter.OP.EQUAL, int(values) if values.isnumeric() else values)
    @classmethod
    def parse_query_string(cls, query_string):
        qs = {**cls.__default_query_string, **{x[0]: x[1] for x in query_string if len(x) == 2}}
        page = qs['page']
        page = int(page) if page.isnumeric() else 0
        length = qs['length']
        length = int(length) if length.isnumeric() else 10
        return (page, length, qs['columns'], qs['db'], qs['names'], qs['types'])
    
    def __init__(self, black_names=None):
        if isinstance(black_names, str) and black_names:
            self.__black_names = {x.strip(): True for x in black_names.split(',')}
        elif isinstance(black_names, (list, tuple)):
            self.__black_names = {x: True for x in black_names if isinstance(x, str) and x}
        else:
            self.__black_names = {}
    def is_accessable(self, name):
        return name not in self.__black_names
    
    @Resource.route('/db/{table}', methods=['GET', 'POST'], need_login=True)
    def access_database_table(self, table, __request__):
        if self.is_accessable(table):
            page, length, columns, dbname, _, _ = self.parse_query_string(__request__.query_string)
            method = __request__.method
            if method == 'GET':
                ok, data, total = accessor.select(table, columns, page=page, length=length, dbname=dbname)
                return self.response(data=data, total=total) if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
            if method == 'POST':
                data = __request__.data
                if 'columns' in data:
                    columns = data['columns']
                    auto_increment_columns = data['auto_increment_columns'] if 'auto_increment_columns' in data else None
                    ok, params = accessor.insert(table, columns, data['data'], auto_increment_columns, dbname)
                    return self.response(params=params) if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, params)
                return self.response(HTTPStatus.BAD_REQUEST)
        return self.response(HTTPStatus.FORBIDDEN)
    @Resource.route('/db/{table}/{values}', methods=['GET', 'PUT', 'DELETE'], need_login=True)
    def access_database_table_rows(self, table, values, __request__):
        if self.is_accessable(table):
            page, length, columns, dbname, names, types = self.parse_query_string(__request__.query_string)
            qf = self.parse_query_filter(values, names, types)
            method = __request__.method
            if method == 'GET':
                ok, data, total = accessor.select(table, columns, qf, page, length, dbname)
                return self.response(data=data, total=total) if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
            if method == 'PUT':
                data = __request__.data
                if 'columns' in data:
                    columns = data['columns']
                    ok, data = accessor.update(table, columns, data['data'], qf, dbname)
                    return self.response() if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
                return self.response(HTTPStatus.BAD_REQUEST)
            if method == 'DELETE':
                ok, data = accessor.delete(table, qf, dbname)
                return self.response() if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
        return self.response(HTTPStatus.FORBIDDEN)
    @Resource.route('/procedure', methods='POST', need_login=True)
    def access_database_procedure(self, __request__):
        data = __request__.data
        if 'name' in data:
            name = data['name']
            if self.is_accessable(name):
                dbname = self.parse_query_string(__request__.query_string)[3]
                reading = not (data['update'] if 'update' in data else False)
                args = tuple(data['params']) if 'params' in data else ()
                ok, data, total, params = accessor.callproc(name, reading, dbname, *args)
                return self.response(data=data, total=total, params=params) if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
            return self.response(HTTPStatus.FORBIDDEN)
        return self.response(HTTPStatus.BAD_REQUEST)
