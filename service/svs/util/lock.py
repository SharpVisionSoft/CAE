'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from errno import EEXIST
from os import open as os_open, close as os_close, unlink as os_unlink, O_CREAT, O_EXCL, O_RDWR
from time import time, sleep
from ..base import Object

class MultiprocessingLock(Object):
    __slots__ = ('__path', '__delay', '__timeout', '__is_locked', '__fd')
    
    def __init__(self, path, delay=.01, timeout=30):
        self.__path = '%s.lck' % path
        self.__delay = delay
        self.__timeout = timeout
        self.__is_locked = False
    def __del__(self):
        self.release()
    def __enter__(self):
        self.acquire()
    def __exit__(self, *_):
        self.release()
    def acquire(self):
        start = time()
        while True:
            try:
                self.__fd = os_open(self.__path, O_CREAT | O_EXCL | O_RDWR)
                break
            except OSError as e:
                if e.errno != EEXIST:
                    raise e
                if (time() - start) >= self.__timeout:
                    raise TimeoutError
                sleep(self.__delay)
        self.__is_locked = True
    def release(self):
        if self.__is_locked:
            self.__is_locked = False
            os_close(self.__fd)
            os_unlink(self.__path)
