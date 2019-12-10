'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from ..base import Object
from ..util.crypto import TokenJWT
from .captcha import GraphicVerification

class UserManager(Object):
    def add(self, username, **kwargs):pass
    def modify(self, username, **kwargs):pass
    def delete(self, username):pass
    def retrieve(self, username=None, page=0, length=10):pass
    def login(self, username, **kwargs):pass
    def logout(self, username):pass
    def active(self, **kwargs):pass
    def is_admin(self, **kwargs):pass
    def is_accessable(self, current_username, **kwargs):pass
    def register(self, username, **kwargs):pass

class UserAuthenticator(UserManager):
    __slots__ = ('__um', '__gv')
    
    __token_generator = TokenJWT(
        b'\xff\xe1\xe4\xdd\x01\xe0W\xf4h\x15\t\x89\xfa\x02\xc5\x8f}\x18\xf4\x19\xdb\xecA\x93.\xec6\xda\xb4L\xc9\x8f\xf4\xb29d2~H\x13\x13\x13o\xf4~\x8b\x82\xc2\x12\xcb\xc2\xd2%T\xf0\x8d\x892f\x9ck\xf8\x00?\x80\xa2U\xf0TE\xc9\xe7\xc2\x1d\xed\xe2\xb8]E\x90\x8c\x80tKtI\xaf\xb6Z\x10\xd2\x0b\x19*\xfcQO\xda\x94\x10\x91\tW4\xf8m0.H\xce*\xac\xdd>\xebA3\xacU10~\\t\xfe\x8c\x1f<',
        b'\x9b\xd1\xcd\x9e^7\x95F\xb2S\xe8\x93\x17\xea\xd08\xe1\xe2\xf2DC\r\xad\x89\x92\x81\x9eJ\xae\xdbH\xf6'
    )
    
    @classmethod
    def generate_token(cls, **kwargs):
        return cls.__token_generator.generate(**kwargs)
    @classmethod
    def verify_token(cls, token):
        return  cls.__token_generator.verify(token)
    
    def __init__(self, user_manager, graphic_verification=None):
        if isinstance(user_manager, UserManager):
            self.__um = user_manager
            self.__gv = graphic_verification if isinstance(graphic_verification, GraphicVerification) else None
        else:
            raise 'invalid UserManager instance'
    def __verify_graphic_code(self, kwargs):
        if self.__gv is None:
            return True
        if 'gcode' in kwargs:
            gcode = kwargs['gcode']
            return isinstance(gcode, dict) and self.__gv.verify(gcode.get('identity', ''), gcode.get('code', ''))
        return False
    def gcode(self):
        if isinstance(self.__gv, GraphicVerification):
            return self.__gv.generate()
        return None
    def add(self, username, **kwargs):
        return self.__um.add(username, **kwargs)
    def modify(self, username, **kwargs):
        return self.__um.modify(username, **kwargs)
    def delete(self, username):
        return self.__um.delete(username)
    def retrieve(self, username=None, page=0, length=10):
        return self.__um.retrieve(username, page, length)
    def login(self, username, **kwargs):
        if self.__verify_graphic_code(kwargs):
            ok, data = self.__um.login(username, **kwargs)
            if ok:
                return (True, {'token': self.generate_token(**data), 'data': data})
            return (False, 'Invalid username or password' if data is None else data)
        return (False, 'Invalid graphic verifying code')
    def logout(self, username):
        return self.__um.logout(username)
    def active(self, **kwargs):
        return self.__um.active(**kwargs)
    def is_admin(self, **kwargs):
        return self.__um.is_admin(**kwargs)
    def is_accessable(self, current_username, **kwargs):
        return self.__um.is_accessable(current_username, **kwargs)
    def register(self, username, **kwargs):
        return self.__um.register(username, **kwargs)
