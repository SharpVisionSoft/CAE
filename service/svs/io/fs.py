'''
Created on Oct 11, 2019

@author: liang.li
'''
from ..base import Object

class FS(Object):
    def exists(self, path):pass
    def listdir(self, path):pass
    def makedirs(self, path):pass
    def status(self, path):pass
    def join(self, *path):pass
    def open(self, path, mod='rb'):pass
    def delete(self, path):pass

class DefaultFS(FS):
    __slots__ = ('__os', '__stat', '__shutil')

    def __init__(self):
        self.__os = __import__('os', fromlist=('listdir', 'makedirs', 'path', 'stat'))
        self.__stat = __import__('stat')
        self.__shutil = __import__('shutil', fromlist=('rmtree',))
    def exists(self, path):
        return self.__os.path.exists(path)
    def listdir(self, path):
        return self.__os.listdir(path)
    def makedirs(self, path):
        return self.__os.makedirs(path)
    def status(self, path):
        st = self.__os.stat(path)
        return (self.__stat.S_ISDIR(st.st_mode), st.st_mtime, st.st_size)
    def join(self, *path):
        return self.__os.path.join(*path)
    def open(self, path, mod='rb'):
        return open(path, mod)
    def delete(self, path):
        self.__shutil.rmtree(path)
        
