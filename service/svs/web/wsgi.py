'''
Created on Sep 26, 2019

@author: liang.li
'''
from http import HTTPStatus
from ..base import Object, Entity
from ..mgr.usrmgr import UserAuthenticator
from ..util.url import URL
from .ws.rs import RoutingRulesMapper, Resource

class Request(Object):
    __slots__ = ('method', 'path', 'query_string', 'data', 'nonce', 'payload')
    
    def __init__(self, method, path, query_string, data, nonce, payload):
        self.method = method
        self.path = path
        self.query_string = query_string
        self.data = data
        self.nonce = nonce
        self.payload = payload

class Application(Object):
    __slots__ = ('__routing_rules_mapper', '__default_headers')
    
    __cors_headers = ('Content-Length', 'Content-Type', 'Token')
    
    @classmethod
    def __parse_charset(cls, ctype):
        charset = None
        ctype_charset = ctype.split(';')
        for x in ctype_charset[1:]:
            if x.strip().startswith('charset'):
                x = x.split('=')
                if len(x) == 2:
                    charset = x[1].strip()
                    break
        return 'utf-8' if charset is None else charset
    @classmethod
    def __parse_data(cls, environ):
        if 'CONTENT_TYPE' in environ and 'CONTENT_LENGTH' in environ and 'wsgi.input' in environ:            
            ctype = environ['CONTENT_TYPE']
            clength = int(environ['CONTENT_LENGTH'])
            creader = environ['wsgi.input']
            ctypel = ctype.lower()
            try:
                if ctypel.startswith('application/json'):
                    return Entity.parse(creader.read(clength).decode(cls.__parse_charset(ctypel)))
                if ctypel.startswith('application/x-www-form-urlencoded'):
                    return URL(creader.read(clength).decode(cls.__parse_charset(ctypel))).query_string
                if ctypel.startswith('text/plain'):
                    return creader.read(clength).decode(cls.__parse_charset(ctypel))
                if ctypel.startswith('multipart/form-data'):
                    return (ctype, clength, creader)
            except:pass
        return None
    @classmethod
    def __do_method(cls, method, path, environ, rule, arg_names, arg_values):
        nonce = None
        payload = None
        if rule.need_login:
            if 'HTTP_TOKEN' in environ:
                ok, nonce, payload = UserAuthenticator.verify_token(environ['HTTP_TOKEN'])
                if not ok:
                    return Resource.response(HTTPStatus.UNAUTHORIZED)
            else:
                return Resource.response(HTTPStatus.UNAUTHORIZED)
        return rule.execute(arg_names, arg_values, Request(method, path, (x.split('=') for x in environ['QUERY_STRING'].split('&')), cls.__parse_data(environ), nonce, payload))
    
    def __init__(self, cors=False):
        self.__routing_rules_mapper = RoutingRulesMapper()
        if cors:
            headers = ','.join(self.__cors_headers)
            self.__default_headers = [
                ('Access-Control-Allow-Headers', headers),
                ('Access-Control-Allow-Methods', ','.join(Resource.__SUPPORTED_METHODS__)),
                ('Access-Control-Allow-Origin', '*'),
                ('Access-Control-Expose-Headers', headers),
                ('Content-Type', 'application/json')
            ]
        else:
            self.__default_headers = [
            ('Content-Type', 'application/json')
        ]
    def __call__(self, environ, start_response):
        try:
            method = environ['REQUEST_METHOD']
            path = environ['PATH_INFO']
            if path.endswith('/'):
                path = path[:-1]
            ok, data = self.__routing_rules_mapper.find(path)
            if ok:
                rule, arg_names, arg_values = data
                if method in rule.methods:
                    response_status, response_data = self.__do_method(method, path, environ, rule, arg_names, arg_values)
                    if isinstance(response_data, tuple):
                        response_headers = self.__default_headers + [('Content-Length', str(response_data[0]))]
                        response_headers.remove(('Content-Type', 'application/json'))
                        response_headers.append(('Content-Type', response_data[1]))
                        start_response(response_status, response_headers)
                        return response_data[2]
                    else:
                        response_headers = [('Content-Length', str(len(response_data)))]
                elif method == 'OPTIONS':
                    allowed = ','.join(rule.methods)
                    if 'GET' in rule.methods:
                        allowed = 'HEAD,' + allowed
                    response_status = '200 OK'
                    response_data = b''
                    response_headers = [
                        ('Allow', allowed),
                        ('Content-Length', '0')
                    ]
                elif method == 'HEAD' and 'GET' in rule.methods:
                    response_status, response_data = self.__do_method('GET', path, environ, rule, arg_names, arg_values)
                    response_headers = [('Content-Length', str(len(response_data)))]
                    response_data = b''
                else:
                    response_status, response_data = Resource.response(HTTPStatus.METHOD_NOT_ALLOWED)
                    response_headers = [
                        ('Allow', ','.join(rule.methods)),
                        ('Content-Length', str(len(response_data)))
                    ]
            else:
                response_status, response_data = Resource.response(HTTPStatus.NOT_FOUND)
                response_headers = [('Content-Length', str(len(response_data)))]
        except Exception as error:
            response_status, response_data = Resource.response(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))
            response_headers = [('Content-Length', str(len(response_data)))]
        start_response(response_status, self.__default_headers + response_headers)
        return [response_data]
    def add_resource(self, resource):
        if isinstance(resource, Resource):
            for rule in resource.resources:
                self.__routing_rules_mapper.add(rule)
