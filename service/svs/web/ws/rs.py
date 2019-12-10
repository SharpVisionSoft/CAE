'''
Created on Sep 25, 2019

@author: liang.li
'''
from http import HTTPStatus
from re import sub as re_sub
from urllib.parse import quote
from ...base import Object, Entity

class RoutingRule(Object):
    __slots__ = ('path', 'args', 'func', 'methods', 'need_login', '_func_instance')
    
    class __Parser(Object):
        __slots__ = ('path', 'args')
        
        def __init__(self, path):
            path = re_sub('[/]{2,}', '/', '/{}'.format(path))
            self.path = path[:-1] if path.endswith('/') else path
            self.args = []
        def matched(self, m):
            self.args.append(m.group(0)[1:-1])
            return '@?'
        def parse(self):
            return (re_sub('\{\w+\}', self.matched, self.path), self.args)
    
    def __init__(self, path, func, methods=None, need_login=False, supported_methods=()):
        if isinstance(path, str) and hasattr(func, '__call__'):
            self.path, self.args = self.__Parser(path).parse()
            self.func = func
            if isinstance(methods, (list, tuple)) and methods:
                self.methods = tuple(x for x in methods if x in supported_methods)
            elif isinstance(methods, str) and methods:
                self.methods = tuple(x.strip() for x in methods.split(',') if x.strip() in supported_methods)
            else:
                self.methods = ()
            self.need_login = bool(need_login)
        else:
            raise ValueError
    def is_supported_method(self, method):
        return method.upper() in self.methods
    def execute(self, arg_names, arg_values, request):
        func = self.func
        fn_code = func.__code__
        flags = fn_code.co_flags
        co_varnames = fn_code.co_varnames
        co_argcount = fn_code.co_argcount
        if flags & 0x04 == 0x04:
            args = []
            start = 0
            if co_argcount > 0 and co_varnames[0] == 'self':
                args.append(self._func_instance)
                start = 1
            for i in range(start, co_argcount):
                name = co_varnames[i]
                if name in arg_values:
                    args.append(arg_values[name])
                    arg_names.remove(name)
                else:
                    args.append(None)
            for name in arg_names:
                args.append(arg_values[name])
            args.append(request)
            return func(*args)
        if flags & 0x08 == 0x08:
            arg_values['__request__'] = request
            if co_argcount > 0 and co_varnames[0] == 'self':
                return func(self._func_instance, **arg_values)
            return func(**arg_values)
        args = []
        start = 0
        if co_argcount > 0 and co_varnames[0] == 'self':
            args.append(self._func_instance)
            start = 1
        for i in range(start, co_argcount):
            name = co_varnames[i]
            if name in arg_values:
                args.append(arg_values[name])
                arg_names.remove(name)
            elif name == '__request__':
                args.append(request)
            else:
                args.append(None)
        return func(*args)

class RoutingRulesMapper(Object):
    __slots__ = ('__map',)
    
    def __init__(self):
        self.__map = {}
    def add(self, rule):
        if isinstance(rule, RoutingRule):
            path = rule.path
            target = self.__map
            for p in path[1:].split('/'):
                if p in target:
                    target = target[p]
                else:
                    t = {}
                    target[p] = t
                    target = t
            if '@rule' in target:
                err = "duplicated routing rule: '{}'".format(path.replace('@?', '{?}'))
                raise ValueError(err)
            target['@rule'] = rule
    def find(self, path):
        path = re_sub('[/]{2,}', '/', '/{}'.format(path))
        ok = True
        args = []
        target = self.__map
        ps = path[1:].split('/')
        for p in ps:
            if p in target:
                target = target[p]
            elif '@?' in target:
                args.append(p)
                target = target['@?']
            else:
                ok = False
                break
        if not (ok and '@rule' in target):
            ok = True
            args = []
            target = self.__map
            for p in ps:
                if p in target:
                    target = target[p]
                elif '@?' in target:
                    args.append(p)
                    target = target['@?']
                elif '*' in target:
                    target = target['*']
                    break
                else:
                    ok = False
                    break
        if ok and '@rule' in target:
            rule = target['@rule']
            if rule is not None:
                arg_names = []
                arg_values = {}
                for n, a in zip(rule.args, args):
                    arg_names.append(n)
                    arg_values[n] = a
                return (True, (rule, arg_names, arg_values))
        return (False, None)

class Resource(Object):
    __SUPPORTED_METHODS__ = ('DELETE', 'GET', 'HEAD', 'OPTIONS', 'POST', 'PUT')
    
    __RESOURCE_ROUTING_RULE = '__RESOURCE_ROUTING_RULE__'
    __RESPONSE_SUCCESS_FORMAT = '{"success":true,"total":%s,"list":%s,"params":%s}'
    __RESPONSE_FAILURE_FORMAT = '{"success":false,"error":"%s"}'
    
    @classmethod
    def route(cls, path, methods=None, need_login=False):
        def decorator(func):
            rule = RoutingRule(path, func, methods, need_login, cls.__SUPPORTED_METHODS__)
            setattr(func, cls.__RESOURCE_ROUTING_RULE, rule)
            return func
        return decorator
    @classmethod
    def response_status(cls, status):
        return '%s %s' % (status.value, getattr(status, 'phrase'))
    @classmethod
    def response(cls, status=HTTPStatus.OK, data=[], total=0, params=()):
        if status == HTTPStatus.OK:
            r = cls.__RESPONSE_SUCCESS_FORMAT % (total, Entity.stringify(data), Entity.stringify(params))
        else:
            r = cls.__RESPONSE_FAILURE_FORMAT % (quote(data) if isinstance(data, str) and data else getattr(status, 'phrase'))
        return (cls.response_status(status), r.encode())
    
    @property
    def resources(self):
        rrr = self.__RESOURCE_ROUTING_RULE
        for name in dir(self):
            if name.startswith('_'):continue
            fn = getattr(self, name)
            if hasattr(fn, '__call__') and hasattr(fn, rrr):
                rule = getattr(fn, rrr)
                rule._func_instance = self
                yield rule
