'''
Created on Nov 20, 2019
@author: Liang.Li
'''
from http import HTTPStatus
from svs.db.query import Filter
from svs.db import accessor
from svs.web.ws.rs_db import Resource

class CAEDatabaseAccessResource(Resource):
    
    __existed_alg = {
        't_user': ('auto_id', ('userId', 'email', None, None, None, None)),
        't_status': ('id', ('userId', 'email', 'moduleId', None, None, None)),
        't_learn_record': ('id', ('userId', 'email', 'moduleId', None, None, None)),
        't_test_record': ('id', ('userId', 'email', 'module_id', 'route_id', 'isfinished', None)),
        't_test_answer_items': ('id', (None, None, None, 'record_id', None, 'stepName'))
    }
    
    @classmethod
    def __parse(cls, table, data, queryString):
        if table in cls.__existed_alg:
            pk, params = cls.__existed_alg[table]
            params = [queryString.get(k, None) or data.get(k, None) if k else None for k in params]
            params.insert(0, table)
            return (True, pk, tuple(params))
        return (False, None, None)
    
    @Resource.route('/cae/sync/{table}', ['GET', 'POST'], need_login=False)
    def sync_data(self, table, __request__):
        table = 't_%s' % table
        if __request__.method == 'GET':
            qs = {**{'page':'0', 'length':'10', 'db':None}, **{x[0]: x[1] for x in __request__.query_string if len(x) == 2}}
            try:
                page = int(qs['page'])
                length = int(qs['length'])
            except:
                page = 0
                length = 10
            ok, data, total = accessor.select(table, page=page, length=length, dbname=qs['db'])
            return self.response(data=data, total=total) if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, data)
        else:
            qs = {**{'userId':None, 'email':None}, **{x[0]: x[1] for x in __request__.query_string if len(x) == 2}}
            data = __request__.data
            if isinstance(data, dict):
                data.pop('isUpload', None)
                ID = data.pop('id', None)
                if table not in ('t_test_record', 't_test_answer_items'):
                    ID = None
                columns = []
                values = []
                for k, v in data.items():
                    columns.append(k)
                    values.append(v)
                columns = ','.join(columns)
                ok, pk, params = self.__parse(table, data, qs)
                if ok:
                    with accessor.Session() as session:
                        if not (isinstance(ID, int) and ID):
                            ok, d, _, _ = session.callproc('p_exists', *params, reading=True)
                            if ok and d[0]:
                                ID = d[0][0].get('id', None)
                        if ID:
                            session.update(table, columns, values, Filter(pk, Filter.OP.EQUAL, ID))
                            return self.response()
                        if table not in ('t_user', 't_test_answer_items'):
                            ok, d, _, _ = session.callproc('p_get_userid', *(qs['userId'], qs['email']), reading=True)
                            if ok and d[0]:
                                columns += ',user_id'
                                values.append(d[0][0].get('id', None))
                        ok, params = session.insert(table, columns, [values], pk)
                        return self.response(params=params) if ok else self.response(HTTPStatus.INTERNAL_SERVER_ERROR, params)
            return self.response(HTTPStatus.BAD_REQUEST)
