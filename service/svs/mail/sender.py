'''
Created on Oct 12, 2019

@author: liang.li
'''
from email.utils import formataddr
from re import match as re_match
from ..base import Object
from .message import MailMessage

class SMTPSender(Object):
    __slots__ = ('__account', '__password', '__host', '__port', '__smtp')
    
    __is_valid_addr = lambda x: bool(isinstance(x, str) and re_match(r'(^[\w-]+(\.[\w-]+)*@[\w-]+(\.[\w-]+)+$)', x))
    __split_addr = lambda x: (x.split('@')[0], x)
    
    @classmethod
    def __parse_addr(cls, addr):
        if isinstance(addr, (list, tuple)) and addr:
            if len(addr) > 1:
                alias, addr = addr[:2]
                if cls.__is_valid_addr(addr):
                    return (addr, formataddr((alias, addr) if isinstance(alias, str) and alias else cls.__split_addr(addr)))
            else:
                addr = addr[0]
                if cls.__is_valid_addr(addr):
                    return (addr, formataddr(cls.__split_addr(addr)))
        elif cls.__is_valid_addr(addr):
            return (addr, formataddr(cls.__split_addr(addr)))
        return (None, None)
    @classmethod
    def __parse_addrs(cls, addrs):
        r = {}
        if isinstance(addrs, str) and addrs:
            for x in addrs.split(','):
                x = x.strip()
                if cls.__is_valid_addr(x):
                    r[x] = formataddr(cls.__split_addr(x))
        elif isinstance(addrs, (list, tuple)) and addrs:
            if len(addrs) == 2 and isinstance(addrs[0], str) and not cls.__is_valid_addr(addrs[0]):
                addr, formated = cls.__parse_addr((addrs[0], addrs[1]))
                if addr:
                    r[addr] = formated
            else:
                for x in addrs:
                    addr, formated = cls.__parse_addr(x)
                    if addr:
                        r[addr] = formated
        return ','.join(x for x in r.values())
    
    def __init__(self, account, password, host, port=25, ssl=False):
        self.__account = account
        self.__password = password
        self.__host = host
        self.__port = port
        self.__smtp = __import__('smtplib', fromlist=('SMTP_SSL',)).SMTP_SSL if ssl else __import__('smtplib', fromlist=('SMTP',)).SMTP
    def send(self, msg, from_addr=None, to_addrs=None, cc_addrs=None):
        if isinstance(msg, MailMessage):
            _, from_addr = self.__parse_addr(from_addr or self.__account)
            to_addrs = self.__parse_addrs(to_addrs)
            cc_addrs = self.__parse_addrs(cc_addrs)
            with self.__smtp(host=self.__host, port=self.__port) as smtp:
                smtp.login(self.__account, self.__password)
                smtp.sendmail(from_addr, to_addrs, msg.as_string(from_addr, to_addrs, cc_addrs))
        else:
            raise 'Invalid MailMessage'
