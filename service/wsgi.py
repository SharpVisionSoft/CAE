'''
Created on Nov 19, 2019
@author: Liang.Li
'''
from os import path as os_path
from sys import path as sys_path

scripts_path = os_path.split(__file__)[0]
if scripts_path not in sys_path:
	sys_path.append(scripts_path)

from app.cae import create_application
application = create_application(os_path.join(scripts_path, 'cae.conf'))
