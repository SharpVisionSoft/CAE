'''
Created on Oct 23, 2019
@author: Liang.Li
'''
from json import dumps, loads
from gzip import decompress
from re import sub as re_sub
from urllib.error import HTTPError
from urllib.parse import unquote, urlencode
from urllib.request import Request, urlopen
from ..base import Object
from ..util.url import URL

class HttpClient(Object):
    __slots__ = ('__base', '__context', '__headers', '__last_headers')
    
    __SSL = __import__('ssl', fromlist=('_create_unverified_context',))
    __HEADERS = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip',
        'Accept-Language': 'en-US,en;q=0.8',
        'User-Agent': 'Skylines HTTP Client/0.0.1'
    }
    
    def __init__(self, url, **headers):
        url = URL(url)
        scheme = url.scheme.lower() if hasattr(url, 'scheme') else 'http'
        host = url.hostname if hasattr(url, 'hostname') else '127.0.0.1'
        port = url.port if hasattr(url, 'port') else 80
        if scheme == 'https':
            port = (443 if port == 80 else port) if isinstance(port, int) and port > 0 else 443
            context = self.__SSL._create_unverified_context()
        else:
            port = (80 if port == 443 else port) if isinstance(port, int) and port > 0 else 80
            context = None
        host = ('%s:%s' % (host, port)) if port != 80 and port != 443 else host
        base = '%s://%s%s' % (scheme, host, url.path)
        headers['Host'] = host
        headers['Referer'] = base
        self.__base = base
        self.__context = context
        self.__headers = dict(**self.__HEADERS, **headers)
        self.__last_headers = None
    def __request(self, method='GET', path=None, query_string=None, data=None, header_only=False):
        url = '%s%s' % (self.__base, re_sub('[/]{2,}', '/', '/{}'.format(path)) if isinstance(path, str) and path else '')
        if isinstance(query_string, dict) and query_string:
            url = '%s?%s' % (url, urlencode(query_string))
        if data is None:
            headers = self.__headers
        else:
            headers = self.__headers.copy()
            if isinstance(data, dict):
                data = dumps(data)
                headers['Content-Type'] = 'application/json; charset=UTF-8'
            else:
                data = str(data)
                headers['Content-Type'] = 'text/plain; charset=UTF-8'
            data = data.encode()
            headers['Content-Length'] = len(data)
        self.__last_headers = None
        try:
            with urlopen(
                Request(url, headers=headers, method=method),
                data=data,
                context=self.__context
            ) as response:
                status = response.getcode()
                headers = dict(response.headers)
                data = None if header_only else response.read()
            msg = 'OK'
            if 'Set-Cookie' in headers:
                self.__headers['Cookie'] = headers['Set-Cookie']
        except HTTPError as e:
            status = e.getcode()
            headers = dict(e.headers)
            data = None if header_only else e.fp.read()
            msg = e.msg
        ctype = headers.get('Content-Type', None)
        if data and ctype:
            ctype_charset = ctype.split(';')
            ctype = ctype_charset[0].strip()
            charset = None
            for x in ctype_charset[1:]:
                if x.strip().lower().startswith('charset'):
                    x = x.split('=')
                    if len(x) == 2:
                        charset = x[1].strip().lower()
                        break
            if charset is None:
                charset = 'utf-8'
            if headers.get('Content-Encoding', '').lower() == 'gzip':
                data = decompress(data)
            try:
                data = unquote(data.decode(charset))
                if ctype == 'application/json':
                    data = loads(data)
            except:pass
        self.__last_headers = headers
        return (status, msg, data)
    def add_header(self, name, value):
        if isinstance(name, str) and isinstance(value, str):
            name = name.strip()
            if name:
                self.__headers[name] = value
    def get_header(self, name):
        return self.__last_headers.get(name, None) if self.__last_headers else None
    def head(self, path):
        status, msg, _ =  self.__request('HEAD', path, header_only=True)
        data = tuple(x for x in self.__last_headers.items()) if status == 200 else None
        return (status, msg, data)
    def options(self, path):
        status, msg, data = self.__request('OPTIONS', path, header_only=True)
        if status == 200:
            headers = self.__last_headers
            return (status, msg, tuple(headers['Allow'].split(',')) if 'Allow' in headers else ())
        return (status, msg, data)
    def get(self, path=None, query_string=None):
        return self.__request('GET', path, query_string=query_string)
    def post(self, path=None, data=None, query_string=None):
        return self.__request('POST', path, query_string=query_string, data=data)
    def put(self, path=None, data=None, query_string=None):
        return self.__request('PUT', path, query_string=query_string, data=data)
    def delete(self, path=None, query_string=None):
        return self.__request('DELETE', path, query_string=query_string)
