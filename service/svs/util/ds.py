'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from threading import RLock, Condition
from ..base import Object

class Stack(Object):
    __slots__ = ('__stack',)
    
    def __init__(self):
        self.__stack = []
    def clear(self):
        self.__stack.clear()
    def peek(self):
        return self.__stack[-1] if self.__stack else None
    def pop(self):
        return self.__stack.pop() if self.__stack else None
    def push(self, item):
        self.__stack.append(item)
    @property
    def is_empty(self):
        return False if self.__stack else True
    @property
    def size(self):
        return len(self.__stack)

class BlockingQueue(Object):
    __slots__ = ('__not_empty', '__not_full', '__maxsize', '__queue')
    
    def __init__(self, maxsize=16):
        lock = RLock()
        self.__not_empty = Condition(lock)
        self.__not_full = Condition(lock)
        self.__maxsize = maxsize if isinstance(maxsize, int) and maxsize > 0 else 16
        self.__queue = []
    def push(self, data):
        with self.__not_full:
            while len(self.__queue) == self.__maxsize:
                self.__not_full.wait()
            self.__queue.append(data)
            self.__not_empty.notify()
    def pop(self):
        with self.__not_empty:
            while not self.__queue:
                self.__not_empty.wait()
            data = self.__queue.pop(0)
            self.__not_full.notify()
        return data
    def clear(self):
        with self.__not_empty:
            self.__queue.clear()
            self.__not_full.notify()
