'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from threading import Event, Thread
from ..base import Object

class Timer(Object):
    __slots__ = ('__interval', '__func', '__args', '__callback', '__loops', '__kwargs', '__event', '__timer')
        
    class __Timer(Thread):
        def __init__(self, func):
            super().__init__()
            self.__func = func
        def run(self):
            self.__func()
    
    def __init__(self, interval, func, *args, callback=None, loops=False, **kwargs):
        self.__interval = interval
        self.__func = func if hasattr(func, '__call__') else lambda *args, **kwargs: (args, kwargs)
        self.__args = args
        self.__callback = callback if hasattr(callback, '__call__') else None
        self.__loops = loops
        self.__kwargs = kwargs
        self.__event = Event()
        self.__timer = self.__Timer(self.__run)
    def __run(self):
        e = self.__event
        try:
            if self.__loops:
                if self.__callback is None:
                    while True:
                        if not e.is_set():
                            e.wait(self.__interval)
                            if e.is_set():break
                            self.__func(*self.__args, **self.__kwargs)
                        else:break
                else:
                    while True:
                        if not e.is_set():
                            e.wait(self.__interval)
                            if e.is_set():break
                            self.__callback(self.__func(*self.__args, **self.__kwargs))
                        else:break
            else:
                e.wait(self.__interval)
                if not e.is_set():
                    if self.__callback is None:
                        self.__func(*self.__args, **self.__kwargs)
                    else:
                        self.__callback(self.__func(*self.__args, **self.__kwargs))
        except:pass
        finally:
            e.set()
    def cancel(self):
        self.__event.set()
    def start(self, daemon=False):
        timer = self.__timer
        timer.setDaemon(daemon)
        timer.start()
        return self
