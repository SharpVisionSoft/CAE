'''
Created on Oct 4, 2019

@author: liang.li
'''
from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from ..util.url import URL
from .ws.rs import Resource

class SimpleWSGIRequestHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.0'
    server_version = 'Skylines WSGIServer/0.0.1'
    
    def __do_method(self, method):
        if hasattr(self.server.application, '__call__'):
            url = URL(self.path)
            environ = {'HTTP_{}'.format(x.upper().replace('-', '_')): self.headers[x] for x in self.headers}
            environ.update(
                REQUEST_METHOD=method,
                PATH_INFO=url.path if hasattr(url, 'path') else '/',
                QUERY_STRING=url.query if hasattr(url, 'query') else ''
            )
            if 'HTTP_CONTENT_TYPE' in environ:
                content_type = environ['HTTP_CONTENT_TYPE']
                del environ['HTTP_CONTENT_TYPE']
            else:
                content_type = None
            if 'HTTP_CONTENT_LENGTH' in environ:
                content_length = environ['HTTP_CONTENT_LENGTH']
                del environ['HTTP_CONTENT_LENGTH']
            else:
                content_length = None
            if content_type and content_length:
                environ.update(**{
                    'CONTENT_TYPE': content_type,
                    'CONTENT_LENGTH': content_length,
                    'wsgi.input': self.rfile
                })
            xs = self.server.application(environ, self.start_response)
            try:
                for x in xs:
                    self.wfile.write(x)
            except:
                if hasattr(xs, 'close'):
                    xs.close()
        else:
            response_status, response_data = Resource.response(HTTPStatus.SERVICE_UNAVAILABLE)
            self.start_response(response_status, [('Content-Length', str(len(response_data)))])
            self.wfile.write(response_data)
    def do_DELETE(self):self.__do_method('DELETE')
    def do_GET(self):self.__do_method('GET')
    def do_HEAD(self):self.__do_method('HEAD')
    def do_OPTIONS(self):self.__do_method('OPTIONS')
    def do_POST(self):self.__do_method('POST')
    def do_PUT(self):self.__do_method('PUT')
    def parse_request(self):
        if super().parse_request():
            script_name = self.server.script_name
            if self.path.startswith(script_name):
                self.path = self.path.replace(script_name, '')
            return True
        return False
    def start_response(self, response_status, response_headers):
        self.send_response(HTTPStatus(int(response_status[:3])))
        for k, v in response_headers:
            self.send_header(k, v)
        self.end_headers()
    #def log_message(self, format, *args):pass

class SimpleWSGIServer(ThreadingHTTPServer):
    __slots__ = ('__application', '__script_name')
    
    @staticmethod
    def serve(host='localhost', port=80, application=None, WSGIScriptAlias='/api'):
        with SimpleWSGIServer(host, port, application, WSGIScriptAlias) as httpd:
            sa = httpd.socket.getsockname()
            print('Serving HTTP on {host} port {port} ...'.format(host=sa[0], port=sa[1]))
            httpd.serve_forever()
        
    def __init__(self, host='localhost', port=80, application=None, WSGIScriptAlias='/api'):
        super().__init__((host, port), RequestHandlerClass=SimpleWSGIRequestHandler)
        self.__application = application
        self.__script_name = WSGIScriptAlias
    @property
    def application(self):
        return self.__application
    @property
    def script_name(self):
        return self.__script_name
