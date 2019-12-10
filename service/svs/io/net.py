'''
Created on Oct 20, 2019

@author: liang.li
'''
from threading import Event
from concurrent.futures.thread import ThreadPoolExecutor 
from ..base import Object, Entity
from ..util.ds import BlockingQueue

class SocketIO(Object):
    def read(self, socket):pass
    def write(self, socket, data):pass

class SocketHandler(Object):
    def on_message(self, message, *args):pass

class Socket(Object):
    __slots__ = ('_s', '_sa', '_io')
    
    __socket_lib = __import__('socket')
    
    @classmethod
    def __create_socket(cls, port, host, stype):
        S = cls.__socket_lib
        stype = S.SOCK_DGRAM if stype == 'UDP' else S.SOCK_STREAM
        s = None
        sa = None
        for af, st, pr, _, sa in S.getaddrinfo(host, port, S.AF_UNSPEC, stype, 0, S.AI_PASSIVE):
            try:
                s = S.socket(af, st, pr)
                try:
                    s.setsockopt(S.SOL_SOCKET, S.SO_REUSEADDR, 1)
                except:
                    s.close()
                    s = None
            except:pass
        if s is None:
            raise OSError('Could not open socket')
        return (s, sa)
    @classmethod
    def _parse(cls, data):
        try:
            data = data.decode(encoding='utf-8')
            data = Entity.parse(data)
        except:pass
        return data
    @classmethod
    def _stringify(cls, data):
        if data is None:
            return b''
        if isinstance(data, bytes):
            return data
        try:
            return Entity.stringify(data).encode(encoding='utf-8')
        except:
            return str(data).encode(encoding='utf-8')
    
    def __init__(self, port, host, stype, ioclass=SocketIO):
        self._io = ioclass()
        self._s, self._sa = self.__create_socket(port, host, stype)
        self._prepare()
    def __del__(self):
        if hasattr(self, '_s'):
            self._s.close()
        self._s = None
        self._sa = None
        self._io = None
    def _prepare(self):pass
    def close(self):
        self._s.close()
    @property
    def _socket_lib(self):
        return self.__socket_lib

class SocketServer(Socket):
    __slots__ = ('__event', '__handler', '__executor', '__queue', '__serving_writing', '__serving_reading')
    
    class __SocketSender(Object):
        __slots__ = ('__s', '__q')
        
        def __init__(self, s, q):
            self.__s = s
            self.__q = q
        def __repr__(self):
            return "{'send': %s}" % type(self.send)
        def send(self, data):
            self.__q.push((self.__s, data))
    
    def __init__(self, port, host, stype, ioclass=SocketIO, handler=None, maxworkers=64, cachesize=64):
        self.__serving_reading = False
        self.__serving_writing = False
        self.__queue = BlockingQueue(maxsize=cachesize if isinstance(cachesize, int) and cachesize > 2 else 64)
        self.__executor = ThreadPoolExecutor(max_workers=maxworkers if isinstance(maxworkers, int) and maxworkers > 2 else 64)
        self.__handler = handler if isinstance(handler, SocketHandler) else SocketHandler()
        self.__event = Event()
        super().__init__(port, host, stype, ioclass)
    def __del__(self):
        self.__event = None
        self.__handler = None
        try:
            self.__executor.shutdown(wait=False)
        except:pass
        self.__executor = None
        self.__queue.clear()
        self.__queue = None
        super().__del__()
    def _prepare(self):
        self._s.bind(self._sa)
    def _do_reading(self, e):pass
    def _do_writing(self, e, q):pass
    def _create_socket_sender(self, s):
        return self.__SocketSender(s, self.__queue)
    def _map(self, fn, *iterables, timeout=None, chunksize=1):
        return self.__executor.map(fn, *iterables, timeout=timeout, chunksize=chunksize)
    def _submit(self, fn, *args, **kwargs):
        return self.__executor.submit(fn, *args, **kwargs)
    def __serve_reading(self):
        self.__serving_reading = True
        try:
            self._do_reading(self.__event)
        except:pass
        self.__serving_reading = False
    def __serve_writing(self):
        self.__serving_writing = True
        try:
            self._do_writing(self.__event, self.__queue)
        except:pass
        self.__serving_writing = False
    def serve(self):
        if not self.__serving_reading:
            self.__executor.submit(self.__serve_reading)
        if not self.__serving_writing:
            self.__executor.submit(self.__serve_writing)
    def serve_forever(self):
        self.serve()
        try:
            while not self.__event.is_set():
                self.__event.wait(1)
        except:pass
    def close(self):
        self.__event.set()
        try:
            self.__executor.shutdown(wait=False)
        except:pass
        super().close()
    @property
    def _handler(self):
        return self.__handler
    @property
    def _queue(self):
        return self.__queue

class SocketClient(Socket):
    def send(self, data):pass
