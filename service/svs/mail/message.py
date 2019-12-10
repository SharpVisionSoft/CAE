'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from os import path as os_path
from email.message import EmailMessage
from mimetypes import guess_type
from ..base import Object

class MailMessage(Object):
    __slots__ = ('subject', 'body', '__attachments')
    
    def __init__(self, subject=None, body=None, attachments=None):
        self.subject = subject
        self.body = body
        self.__set_attachments(attachments)
    def attributes(self, names=None):
        attrs = super().attributes(names)
        if names is None or (isinstance(names, (str, list, tuple)) and 'attachments' in names):
            attrs['attachments'] = self.attachments
        return attrs
    def __set_attachments(self, attachments):
        if isinstance(attachments, str) and attachments:
            self.__attachments = set(x.strip() for x in attachments.split(',') if os_path.isfile(x.strip()))
        elif isinstance(attachments, (list, tuple)):
            self.__attachments = set(x for x in attachments if isinstance(x, str) and os_path.isfile(x))
        else:
            self.__attachments = set()
    def _prepare(self, msg):pass
    def add_attachment(self, attachment):
        if isinstance(attachment, str) and os_path.isfile(attachment):
            self.__attachments.add(attachment)
    def as_string(self, from_addr, to_addrs, cc_addrs):
        m = EmailMessage()
        m['Subject'] = self.subject
        if from_addr:
            m['From'] = from_addr
        if to_addrs:
            m['To'] = to_addrs
        if cc_addrs:
            m['Cc'] = cc_addrs
        self._prepare(m)
        for a in self.__attachments:
            filename = os_path.split(a)[1]
            maintype, subtype = (guess_type(filename)[0] or 'application/octet-stream').split('/', 1)
            with open(a, 'rb') as fp:
                m.add_attachment(fp.read(), maintype=maintype, subtype=subtype, filename=filename)
        return m.as_string()
    @property
    def attachments(self):
        return tuple(x for x in self.__attachments)
    @attachments.setter
    def attachments(self, value):
        self.__set_attachments(value)

class MailMessageText(MailMessage):
    def _prepare(self, msg):
        b = self.body
        if isinstance(b, str) and b:
            msg.add_alternative(b, subtype='plain')

class MailMessageHTML(MailMessage):
    __slots__ = ('__related',)
    
    __cid_format = 'cid:%s'
    
    def __init__(self, subject=None, body=None, attachments=None, **related):
        super().__init__(subject, body, attachments)
        self.related = related
    def attributes(self, names=None):
        attrs = super().attributes(names)
        if names is None or (isinstance(names, (str, list, tuple)) and 'related' in names):
            attrs['related'] = self.related
        return attrs
    def add_related(self, name, file_path):
        if os_path.isfile(file_path) and self.body.find(self.__cid_format % name) >= 0:
            self.__related[name] = file_path
    def _prepare(self, msg):
        b = self.body
        if isinstance(b, str) and b:
            r = self. __related
            if r:
                cids = {x:'x{:0>4}'.format(i) for i, x in enumerate(set(r.values()))}
                for name, path in r.items():
                    b = b.replace(self.__cid_format % name, self.__cid_format % cids[path])
                msg.add_alternative(b, subtype='html')
                payload = msg.get_payload()[0]
                for path, cid in cids.items():
                    mtyp, styp = (guess_type(path)[0] or 'application/octet-stream').split('/', 1)
                    with open(path, 'rb') as f:
                        data = f.read()
                    payload.add_related(data, mtyp, styp, cid=cid)
            else:
                msg.add_alternative(b, subtype='html')
    @property
    def related(self):
        return tuple((k, v) for k, v in self.__related.items())
    @related.setter
    def related(self, value):
        self.__related = {k: v for k, v in value.items() if os_path.isfile(v) and self.body.find(self.__cid_format % k) >= 0} if isinstance(value, dict) else {}
