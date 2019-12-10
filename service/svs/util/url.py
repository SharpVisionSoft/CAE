'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from ..base import Entity

class URL(Entity):
    __slots__ = ('scheme', 'username', 'password', 'hostname', 'port', 'path', 'params', 'query', 'fragment')
        
    def __init__(self, url=None):
        if isinstance(url, str) and url:
            self.__initialize(urlparse(unquote(url)))
        elif isinstance(url, URL):
            self.__initialize(url)
    def __initialize(self, r):
        self.scheme = r.scheme
        self.username = r.username
        self.password = r.password
        self.hostname = r.hostname
        self.port = r.port
        self.path = r.path
        self.params = r.params
        self.query = r.query
        self.fragment = r.fragment
    def __repr__(self):
        return '%s://%s%s%s%s%s%s' % (
            self.scheme if hasattr(self, 'scheme') else '',
            ('%s:%s@' % (self.username, self.password) if hasattr(self, 'password') and self.password else '%s@' % self.username) if hasattr(self, 'username') and self.username else '',
            ('%s:%s' % (self.hostname, self.port) if hasattr(self, 'port') and self.port else '%s' % self.hostname) if hasattr(self, 'hostname') and self.hostname else '',
            ('%s' if self.path.startswith('/') else '/%s') % self.path if hasattr(self, 'path') and isinstance(self.path, str) and self.path else '',
            ';%s' % self.params if hasattr(self, 'params') and self.params else '',
            '?%s' % self.query if hasattr(self, 'query') and self.query else '',
            '#%s' % self.fragment if hasattr(self, 'fragment') and self.fragment else ''
        )
    @property
    def query_string(self):
        return {k: v[-1] for k, v in parse_qs(self.query).items()} if hasattr(self, 'query') and self.query else {}
    @query_string.setter
    def query_string(self, value):
        if isinstance(value, dict):
            self.query = unquote(urlencode(value))
    @property
    def quoted(self):
        return '%s://%s%s%s%s%s%s' % (
            self.scheme if hasattr(self, 'scheme') else '',
            ('%s:%s@' % (quote(self.username), quote(self.password)) if hasattr(self, 'password') and self.password else '%s@' % quote(self.username)) if hasattr(self, 'username') and self.username else '',
            ('%s:%s' % (quote(self.hostname), self.port) if hasattr(self, 'port') and self.port else '%s' % quote(self.hostname)) if hasattr(self, 'hostname') and self.hostname else '',
            ('%s' if self.path.startswith('/') else '/%s') % quote(self.path) if hasattr(self, 'path') and isinstance(self.path, str) and self.path else '',
            ';%s' % quote(self.params) if hasattr(self, 'params') and self.params else '',
            '?%s' % urlencode(self.query_string) if hasattr(self, 'query') and self.query else '',
            '#%s' % quote(self.fragment) if hasattr(self, 'fragment') and self.fragment else ''
        )
