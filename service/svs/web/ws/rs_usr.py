'''
Created on Sep 29, 2019
@author: Liang.Li
'''
from http import HTTPStatus
from ...mgr.usrmgr import UserAuthenticator
from .rs import Resource

class UserManagerResource(Resource):
    __slots__ = ('__authenticator',)
    
    @classmethod
    def __analyze_data(cls, data):
        ok, data = data
        if ok:
            if isinstance(data, (list, tuple)):
                return cls.response(data=data, total=len(data))
            if isinstance(data, dict):
                return cls.response(data=[data], total=1)
            return cls.response()
        return cls.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
    
    def __init__(self, user_manager, graphic_verification=None):
        self.__authenticator = UserAuthenticator(user_manager, graphic_verification)
    
    @Resource.route('/users', methods=['GET', 'POST'], need_login=True)
    def add_or_get_user(self, __request__):
        if self.__authenticator.is_admin(**__request__.payload):
            method = __request__.method
            if method == 'GET':
                data = {x[0]: x[1] for x in __request__.query_string if len(x) == 2}
                r = self.__authenticator.retrieve(page=int(data.get('page', '0')), length=int(data.get('length', '10')))
                if r is None:
                    return self.response(HTTPStatus.NOT_IMPLEMENTED)
                ok, data, total = r
                if ok:
                    return self.response(data=data, total=total)
                return self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
            if method == 'POST':
                data = __request__.data
                if isinstance(data, dict) and data:
                    username = data.pop('username', None)
                    if username is not None and data:
                        r = self.__authenticator.add(username, **data)
                        if r is None:
                            return self.response(HTTPStatus.NOT_IMPLEMENTED)
                        return self.__analyze_data(r)
                return self.response(HTTPStatus.BAD_REQUEST)
        return self.response(HTTPStatus.FORBIDDEN)
    @Resource.route('/users/{username}', methods=['GET', 'PUT', 'DELETE'], need_login=True)
    def get_or_put_or_delete_user(self, username, __request__):
        method = __request__.method
        if method == 'GET':
            if self.__authenticator.is_accessable(username, **__request__.payload):
                r = self.__authenticator.retrieve(username)
                if r is None:
                    return self.response(HTTPStatus.NOT_IMPLEMENTED)
                ok, data, total = r
                if ok:
                    return self.response(data=data, total=total)
                return self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
        if method == 'PUT':
            if self.__authenticator.is_accessable(username, **__request__.payload):
                data = __request__.data
                if isinstance(data, dict):
                    data.pop('username', None)
                    if data:
                        r = self.__authenticator.modify(username, **data)
                        if r is None:
                            return self.response(HTTPStatus.NOT_IMPLEMENTED)
                        return self.__analyze_data(r)
                return self.response(HTTPStatus.BAD_REQUEST)
        if method == 'DELETE':
            if self.__authenticator.is_admin(**__request__.payload):
                r = self.__authenticator.delete(username)
                if r is None:
                    return self.response(HTTPStatus.NOT_IMPLEMENTED)
                return self.__analyze_data(r)
        return self.response(HTTPStatus.FORBIDDEN)
    @Resource.route('/users/{username}/login', methods=['POST'])
    def login(self, username, __request__):
        data = __request__.data
        if isinstance(data, dict) and data:
            data.pop('username', None)
            r = self.__authenticator.login(username, **data)
            if r is None:
                return self.response(HTTPStatus.NOT_IMPLEMENTED)
            ok, data = r
            if ok:
                return self.response(data=[data], total=1)
            return self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
        return self.response(HTTPStatus.BAD_REQUEST)
    @Resource.route('/users/{username}/logout', methods=['POST'], need_login=True)
    def logout(self, username):
        r = self.__authenticator.logout(username)
        if r is None:
            return self.response(HTTPStatus.NOT_IMPLEMENTED)
        return self.__analyze_data(r)
    @Resource.route('/user/gcode', methods=['POST'])
    def gcode(self, __request__):
        r = self.__authenticator.gcode()
        if r is None:
            return self.response(HTTPStatus.NOT_IMPLEMENTED)
        return self.response(data=[{'identity': r[0], 'image': r[1]}], total=1)
    @Resource.route('/user/register', methods=['POST'])
    def register(self, __request__):
        data = __request__.data
        if isinstance(data, dict) and data:
            username = data.pop('username', None)
            if username is not None and data:
                r = self.__authenticator.register(username, **data)
                if r is None:
                    return self.response(HTTPStatus.NOT_IMPLEMENTED)
                return self.__analyze_data(r)
        return self.response(HTTPStatus.BAD_REQUEST)
    @Resource.route('/user/activation', methods=['PUT'])
    def activate(self, __request__):
        data = __request__.data
        if isinstance(data, dict) and data:
            r = self.__authenticator.active(**data)
            if r is None:
                return self.response(HTTPStatus.NOT_IMPLEMENTED)
            return self.__analyze_data(r)
        return self.response(HTTPStatus.BAD_REQUEST)
