'''
Created on Nov 20, 2019
@author: Liang.Li
'''
from svs.util.xml import XMLReader
from svs.db.mgr import ConnectionManager
from svs.web.wsgi import Application
from .res import CAEDatabaseAccessResource

def create_application(conf):
    conf = XMLReader(conf).read()
    for tag in conf.find('database', 'connection'):
        attrs = tag['attrs']
        name = attrs.get('name', None)
        try:
            maxsize = int(attrs.get('pool_size', '8'))
        except:
            maxsize = 8
        readonly = attrs.get('readonly', 'no').lower()
        ConnectionManager.set_pool(name, maxsize, readonly in ('yes', 'true', '1'), url=tag['text'])
    application = Application(cors=True)
    application.add_resource(CAEDatabaseAccessResource())
    return application
