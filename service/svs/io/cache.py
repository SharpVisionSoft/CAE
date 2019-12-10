'''
Created on Sep 29, 2019

@author: liang.li
'''
from base64 import b64encode
from binascii import a2b_hex
from os import listdir as os_listdir, makedirs as os_makedirs, path as os_path, unlink as os_unlink
from shutil import rmtree
from time import time, sleep
from threading import RLock
from ..base import Object, Entity
from ..util.lock import MultiprocessingLock
from ..util.timer import Timer
from .netstream import StreamHandler, SimpleStreamServer, SimpleStreamClient

class MemoryCache(Object):
    def put(self, k, v, ex=0):pass
    def get(self, k):pass
    def pop(self, k):pass

class ThreadingMemoryCache(MemoryCache):
    __slots__ = ('__cache', '__lock', '__timer')
        
    def __init__(self, cleaning_interval=300):
        self.__cache = {}
        self.__lock = RLock()
        self.__timer = Timer(cleaning_interval / 5, self.__do_cleaning, *(self.__lock, self.__cache), loops=True).start(daemon=True)
    def __del__(self):
        self.__timer.cancel()
    def __do_cleaning(self, lock, cache):
        with lock:
            keys = [x for x in cache]
        print(keys)
        for k in keys:
            with lock:
                if k in cache:
                    _, ex, tm = cache[k]
                    if ex > 0 and time() - tm >= ex:
                        del cache[k]
            sleep(.05)
    def put(self, k, v, ex=0):
        with self.__lock:
            self.__cache[k] = (v, ex, time())
        return True
    def get(self, k):
        with self.__lock:
            if k in self.__cache:
                v, ex, tm = self.__cache[k]
                if ex > 0 and time() - tm >= ex:
                    del self.__cache[k]
                    v = None
        return v
    def pop(self, k):
        with self.__lock:
            if k in self.__cache:
                v, ex, tm = self.__cache[k]
                return None if ex > 0 and time() - tm >= ex else v
        return None

class MultiprocessingMemoryCache(MemoryCache):
    __slots__ = ('__path', '__timer')
    
    def __init__(self, path='./', timeout=300):
        path = os_path.join(path, '93b1bf9afecb11e9b21e085700f81efe')
        if os_path.exists(path):
            if not os_path.isdir(path):
                rmtree(path)
                os_makedirs(path)
        else:
            os_makedirs(path)
        self.__path = path
        self.__timer = Timer(timeout / 5, self.__do_cleaning, path, loops=True).start(daemon=True)
    def __del__(self):
        self.__timer.cancel()
    def __do_cleaning(self, path):
        now = time()
        for filename in os_listdir(path):
            filename = os_path.join(path, filename)
            try:
                with MultiprocessingLock(filename):
                    with open(filename, 'rb') as f:
                        v = f.read()
                    ok, ex, tm, _ = self.__parse_value(v)
                    if ok and ex > 0 and now - tm >= ex:
                        os_unlink(filename)
            except:pass
            sleep(.05)
    def __key(self, k):
        try:
            a2b_hex(k)
        except:
            k = b64encode(k.encode()).hex()
        return os_path.join(self.__path, '%s.dat' % k)
    def __parse_value(self, v):
        if v:
            v = v.decode().split('|', 2)
            if len(v) == 3:
                return (True, int(v[0]), float(v[1]), v[2])
        return (False, None, None, None)
    def __get_or_pop(self, k, b):
        k = self.__key(k)
        if os_path.isfile(k):
            with MultiprocessingLock(k):
                with open(k, 'rb') as f:
                    v = f.read()
                if b:os_unlink(k)
            ok, ex, tm, v = self.__parse_value(v)
            if ok:
                if ex > 0 and time() - tm >= ex:
                    return None
                return Entity.parse(v)
        return None
    def put(self, k, v, ex=0):
        k = self.__key(k)
        v = ('%s|%s|%s' % (ex if isinstance(ex, int) and ex > 0 else 0, time(), Entity.stringify(v))).encode()
        with MultiprocessingLock(k):
            with open(k, 'wb') as f:
                f.write(v)
        return True
    def get(self, k):
        return self.__get_or_pop(k, False)
    def pop(self, k):
        return self.__get_or_pop(k, True)

class RemoteCacheServer(ThreadingMemoryCache, StreamHandler):
    __slots__ = ('__server',)
    
    def __init__(self, port, host='0.0.0.0', maxworkers=64, cachesize=64, cleaning_interval=300):
        ThreadingMemoryCache.__init__(self, cleaning_interval)
        self.__server = SimpleStreamServer(port, host, self, maxworkers, cachesize)
    def __del__(self):
        self.__server.close()
        super().__del__()
    def on_message(self, message, *args):
        d = {'ok': False}
        if isinstance(message, dict) and 'op' in message and 'k' in message:
            op = message['op']
            k = message['k']
            if op == 0:
                self.put(k, message.get('v', None), message.get('ex', 0))
                d['ok'] = True
            elif op == 1:
                d['ok'] = True
                d['v'] = self.get(k)
            elif op == 2:
                d['ok'] = True
                d['v'] = self.pop(k)
        args[0].send(d)
    def serve(self):
        self.__server.serve()
    def serve_forever(self):
        self.__server.serve_forever()
    def close(self):
        self.__server.close()

class RemoteCacheClient(MemoryCache):
    __slots__ = ('__client',)
    
    def __init__(self, port, host='127.0.0.1'):
        self.__client = SimpleStreamClient(port, host)
    def put(self, k, v, ex=0):
        r = self.__client.send({'op': 0, 'k': k, 'v': v, 'ex': ex})
        return r.get('ok', False)
    def get(self, k):
        r = self.__client.send({'op': 1, 'k': k})
        return r.get('v', None) if r.get('ok', False) else None
    def pop(self, k):
        r = self.__client.send({'op': 2, 'k': k})
        return r.get('v', None) if r.get('ok', False) else None
