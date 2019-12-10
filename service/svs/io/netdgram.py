'''
Created on Oct 20, 2019

@author: liang.li
'''
from select import select
from .net import SocketIO, SocketServer, SocketClient

class DgramIO(SocketIO):
    def read(self, socket):
        return socket.recvfrom(1024)
    def write(self, socket, data):
        data, addr = data
        try:
            socket.sendto(data, addr)
            return True
        except:
            return False

class DgramServer(SocketServer):
    def __init__(self, port, host='0.0.0.0', ioclass=DgramIO, handler=None, maxworkers=64, cachesize=64):
        super().__init__(port, host, 'UDP', ioclass, handler, maxworkers, cachesize)
    def _prepare(self):
        super()._prepare()
        self._s.setblocking(False)
    def _on_message(self, data, sa):
        try:
            self._handler.on_message(self._parse(data), self._create_socket_sender(sa), sa)
        except:pass
    def _do_reading(self, e):
        s = self._s
        l0 = (s,)
        l1 = ()
        while not e.is_set():
            r, _, x = select(l0, l1, l0)
            if s in r:
                data, sa = self._io.read(s)
                if data:
                    self._submit(self._on_message, data, sa)
                else:
                    break
            elif s in x:
                break
    def _do_writing(self, e, q):
        s = self._s
        l0 = (s,)
        l1 = ()
        while not e.is_set():
            sa, data = q.pop()
            _, w, x = select(l1, l0, l0)
            if s in w:
                self._io.write(s, (self._stringify(data), sa))
            elif s in x:
                break

class DgramClient(SocketClient):
    def __init__(self, port, host='127.0.0.1', ioclass=DgramIO):
        super().__init__(port, host, 'UDP', ioclass)
    def send(self, data):
        self._io.write(self._s, (self._stringify(data), self._sa))
        d, _ = self._io.read(self._s)
        return self._parse(d)

class Multicast(DgramServer):
    __slots__ = ('__group',)
    
    __struct_lib = __import__('struct', fromlist=('pack',))
    
    def __init__(self, port=34567, host='0.0.0.0', group='234.50.61.72', ioclass=DgramIO, handler=None, maxworkers=64, cachesize=64):
        self.__group = (group, port if isinstance(port, int) and port > 1024 else 34567)
        super().__init__(port, host, ioclass, handler, maxworkers, cachesize)
    def _prepare(self):
        S = self._socket_lib
        mreq = self.__struct_lib.pack('=4sl', S.inet_aton(self.__group[0]), S.INADDR_ANY)
        self._s.setsockopt(S.IPPROTO_IP, S.IP_ADD_MEMBERSHIP, mreq)
        super()._prepare()
    def _on_message(self, data, sa):
        try:
            self._handler.on_message(self._parse(data), sa)
        except:pass
    def send(self, data):
        self._queue.push((self.__group, data))

class BroadcastServer(DgramServer):pass
class BroadcastClient(DgramClient):
    __slots__ = ('__target',)
    
    def __init__(self, port, host='127.0.0.1', ioclass=DgramIO):
        self.__target = ('<broadcast>', port)
        super().__init__(port, host, ioclass)
    def _prepare(self):
        S = self._socket_lib
        self._s.setsockopt(S.SOL_SOCKET, S.SO_BROADCAST, 1)
        super()._prepare()
    def send(self, data):
        self._io.write(self._s, (self._stringify(data), self.__target))
        d, _ = self._io.read(self._s)
        return self._parse(d)
