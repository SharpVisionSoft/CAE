'''
Created on Oct 21, 2019

@author: liang.li
'''
from base64 import b64encode
from hashlib import sha1
from os import urandom
from select import select
from struct import pack, unpack
from threading import Event, Thread
from ..util.url import URL
from ..io.netstream import StreamClosedSignal, StreamIO, StreamHandler, StreamServer, StreamClient

class WebSocketIO(StreamIO):
    __slots__ = ('__c',)
    
    def __init__(self):
        self.__c = {}
    def read(self, socket):
        try:
            d = socket.recv(2)
            opcode = d[0] & 127
            if opcode == 8:
                return StreamClosedSignal
            if opcode == 9:
                self.__queue.push((socket, b'\x8a\x00'))
            else:
                is_eof = (d[0] >> 7) > 0
                has_mask = (d[1] >> 7) > 0
                size = d[1] & 127
                if size == 126:
                    size = unpack('!H', socket.recv(2))[0]
                elif size == 127:
                    size = unpack('!Q', socket.recv(8))[0]
                if has_mask:
                    mask = socket.recv(4)
                    bs = socket.recv(size)
                    data = bytearray()
                    for i in range(len(bs)):
                        data.append(bs[i] ^ mask[i % 4])
                else:
                    data = socket.recv(size)
                key = str(hash(socket))
                bs = self.__c.pop(key, b'')
                if is_eof:
                    return bs + data
                self.__c.setdefault(key, bs + data)
            return None
        except:
            return StreamClosedSignal
    def write(self, socket, data, mask=False):
        size = len(data)
        maxsize = 2 ** 64 - 1
        if 0 < size <= maxsize:
            bs = b'\x81'
            if mask:
                if size <= 125:
                    bs += pack('B', (1 << 7) | size)
                elif size <= 2 ** 16 - 1:
                    bs += pack('!BH', (1 << 7) | 126, size)
                else:
                    bs += pack('!BQ', (1 << 7) | 127, size)
                mask = urandom(4)
                bs += mask
                d = bytearray()
                for i in range(size):
                    d.append(data[i] ^ mask[i % 4])
                data = d
            else:
                if size <= 125:
                    bs += pack('B', size)
                elif size <= 2 ** 16 - 1:
                    bs += pack('!BH', 126, size)
                else:
                    bs += pack('!BQ', 127, size)
            try:
                socket.send(bs + data)
            except:
                return False
        return True

class WebSocketServer(StreamServer):
    __magic_string = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
    __response_str = 'HTTP/1.1 101 Switching Protocols\r\nUpgrade:websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\nWebSocket-Location: ws://%s%s\r\n\r\n'
    
    @classmethod
    def __parse_headers(cls, data):
        headers = {}
        try:
            data = data.decode(encoding='utf-8')
            header = data.split('\r\n\r\n', 1)[0]
            for i in header.split('\r\n'):
                ii = i.split(':', 1)
                if len(ii) >= 2:
                    headers[ii[0]] = ''.join(ii[1:]).strip()
                else:
                    ii = i.split(' ')
                    if len(ii) == 3:
                        headers['method'] = ii[0]
                        headers['path'] = ii[1]
                        headers['protocol'] = ii[2]
        except:pass
        return headers
    
    def __init__(self, port, host='0.0.0.0', handler=None, maxworkers=64, cachesize=64):
        super().__init__(port, host, WebSocketIO, handler, maxworkers, cachesize)
    def _shake(self, s):
        try:
            l = (s,)
            r, _, _ = select(l, (), l)
            if s in r:
                headers = self.__parse_headers(s.recv(8192))
                key = headers.get('Sec-WebSocket-Key', None)
                if key:
                    key += self.__magic_string
                    key = b64encode(sha1(key.encode('utf-8')).digest())
                    res = self.__response_str % (key.decode('utf-8'), headers.get('Host', ''), headers.get('path', ''))
                    _, r, _ = select((), l, l)
                    if s in r:
                        s.send(res.encode(encoding='utf-8'))
                        return True
        except:pass
        return False

class WebSocketClient(StreamClient):
    __slots__ = ('__url', '__handler', '__event')
    
    __headers = 'GET %s HTTP/1.1\r\nHost: %s:%s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: %s\r\n\r\n'
    
    def __init__(self, url, handler=None):
        url = URL(url)
        self.__url = url
        self.__handler = handler if isinstance(handler, StreamHandler) else StreamHandler()
        self.__event = Event()
        super().__init__(url.port, url.hostname, WebSocketIO)
    def __do_reading(self):
        s = self._s
        e = self.__event
        l0 = (s,)
        l1 = ()
        try:
            while not e.is_set():
                r, _, x = select(l0, l1, l0)
                if s in r:
                    d = self._io.read(s)
                    if d is StreamClosedSignal:
                        break
                    elif d:
                        self.__handler.on_message(self._parse(d))
                elif s in x:
                    break
        except:pass
        s.close()
        self.__handler.on_close()
    def _shake(self):
        s = self._s
        s.setblocking(False)
        url = self.__url
        d = self.__headers % (url.path if url.path else '/', url.hostname, url.port, b64encode(urandom(16)).decode())
        l0 = (s,)
        l1 = ()
        _, w, _ = select(l1, l0, l0)
        if s in w:
            s.send(d.encode())
            r, _, _ = select(l0, l1, l0)
            if s in r:
                d = s.recv(8192)
                if d:
                    t = Thread(target=self.__do_reading)
                    t.daemon = True
                    t.start()
                    try:
                        self.__handler.on_connect()
                    except:pass
                    return
        raise 'Cannot connect remote server'
    def serve_forever(self):
        e = self.__event
        try:
            while not e.is_set():
                e.wait(1)
        except:pass
    def send(self, data):
        s = self._s
        l = (s,)
        _, w, _ = select((), l, l)
        if s in w:
            return self._io.write(s, self._stringify(data))
        return False
