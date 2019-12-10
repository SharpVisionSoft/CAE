'''
Created on Oct 20, 2019

@author: liang.li
'''
from select import select
from threading import RLock, Thread
from ..base import Object
from .net import SocketIO, SocketHandler, SocketServer, SocketClient

class StreamClosedSignal(Object):
    def __init__(self):raise

class StreamIO(SocketIO):
    def read(self, socket):
        d = socket.recv(4096)
        return d if d else StreamClosedSignal
    def write(self, socket, data):
        try:
            socket.send(data)
            return True
        except:
            return False

class StreamHandler(SocketHandler):
    def on_connect(self, *args):pass
    def on_close(self, *args):pass

class StreamServer(SocketServer):
    __slots__ = ('__lock', '__clients')
    
    __request_queue_size = 5
    
    def __init__(self, port, host='0.0.0.0', ioclass=StreamIO, handler=None, maxworkers=64, cachesize=64):
        self.__clients = {}
        self.__lock = RLock()
        super().__init__(port, host, 'TCP', ioclass, handler if isinstance(handler, StreamHandler) else StreamHandler(), maxworkers, cachesize)
    def __del__(self):
        self.__lock = None
        for v in self.__clients.values():
            v[0].close()
        self.__clients.clear()
        self.__clients = None
        super().__del__()
    def _shake(self, s):
        return bool(s)
    def __all_clients(self):
        with self.__lock:
            return [x[0] for x in self.__clients.values()]
    def __on_message(self, s):
        try:
            d = self._io.read(s)
            if d is StreamClosedSignal:
                return False
            if d:
                d = self._parse(d)
                with self.__lock:
                    c = self.__clients.get(str(hash(s)), None)
                if c:
                    try:
                        self._handler.on_message(d, c[2], c[1])
                    except:pass
            return True
        except:pass
        return False
    def __on_connect(self, s, sa):
        s.setblocking(False)
        if self._shake(s):
            sender = self._create_socket_sender(s)
            with self.__lock:
                self.__clients[str(hash(s))] = (s, sa, sender)
            try:
                self._handler.on_connect(sender, sa)
            except:pass
    def __on_close(self, s):
        with self.__lock:
            c = self.__clients.pop(str(hash(s)), None)
        if c:
            try:
                self._handler.on_close(c[2], c[1])
            except:pass
        s.close()
    def __do_reading(self, e):
        l1 = ()
        while not e.is_set():
            l0 = self.__all_clients()
            if l0:
                r, _, x = select(l0, l1, l0)
                if r:
                    x = list(x) + [s for s, ok in zip(r, self._map(self.__on_message, r)) if not ok]
                if x:
                    self._map(self.__on_close, x)
    def _do_reading(self, e):
        self._submit(self.__do_reading, e)
        self._s.listen(self.__request_queue_size)
        while not e.is_set():
            cli = self._s.accept()
            try:
                t = Thread(target=self.__on_connect, args=cli)
                t.daemon = True
                t.start()
            except:pass
    def _do_writing(self, e, q):
        l1 = ()
        while not e.is_set():
            s, d = q.pop()
            if s is None:
                l0 = self.__all_clients()
            elif s in self.__all_clients():
                l0 = [s]
            else:
                l0 = []
            while l0:
                _, w, x = select(l1, l0, l0)
                if w:
                    wx = []
                    for s, ok in zip(w, self._map(self._io.write, w, (self._stringify(d),) * len(w))):
                        if ok:
                            l0.remove(s)
                        else:
                            wx.append(s)
                    if wx:
                        x = list(x) + wx
                if x:
                    for s, _ in zip(x, self._map(self.__on_close, x)):
                        l0.remove(s)
    def broadcast(self, data):
        self._queue.push((None, data))

class StreamClient(SocketClient):   
    def __init__(self, port, host='127.0.0.1', ioclass=StreamIO):
        super().__init__(port, host, 'TCP', ioclass)
    def _shake(self):pass
    def _prepare(self):
        self._s.connect(self._sa)
        self._shake()
    def send(self, data):
        self._io.write(self._s, self._stringify(data))
        return self._parse(self._io.read(self._s))

class SimpleStreamIO(StreamIO):
    __auth_key = b'\x53\x4b\x59\x4c\x49\x4e\x45\x53\xfd\xcc\x86\x1d\xdc\x9a\x51\xb9'
    __struct_lib = __import__('struct', fromlist=('pack', 'unpack'))
    
    @classmethod
    def auth_key(cls):
        return cls.__auth_key
    
    def read(self, socket):
        unpack = self.__struct_lib.unpack
        try:
            size = unpack('B', socket.recv(1))[0]
            if size == 125:
                size = unpack('!H', socket.recv(2))[0]
            elif size == 126:
                size = unpack('!I', socket.recv(4))[0]
            elif size == 127:
                size = unpack('!Q', socket.recv(8))[0]
            return socket.recv(size) if size > 0 else None
        except:
            return StreamClosedSignal
    def write(self, socket, data):
        pack = self.__struct_lib.pack
        size = len(data)
        if size > 0:
            if size <= 124:
                bs = pack('B', size)
            elif size <= 2 ** 16 - 1:
                bs = pack('!BH', 125, size)
            elif size <= 2 ** 32 - 1:
                bs = pack('!BI', 126, size)
            elif size <= 2 ** 64 - 1:
                bs = pack('!BQ', 127, size)
            else:
                return True
            return super().write(socket, bs + data)
        return True

class SimpleStreamServer(StreamServer):
    def __init__(self, port, host='0.0.0.0', handler=None, maxworkers=64, cachesize=64):
        super().__init__(port, host, SimpleStreamIO, handler, maxworkers, cachesize)
    def _shake(self, s):
        try:
            l = (s,)
            r, _, _ = select(l, (), l)
            if s in r:
                key = SimpleStreamIO.auth_key()
                return s.recv(len(key)) == key
        except:pass
        return False

class SimpleStreamClient(StreamClient):
    def __init__(self, port, host='127.0.0.1'):
        super().__init__(port, host, SimpleStreamIO)
    def _shake(self):
        self._s.send(SimpleStreamIO.auth_key())
