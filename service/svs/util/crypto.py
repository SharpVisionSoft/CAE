'''
Created on Oct 8, 2019
@author: Liang.Li
'''
from base64 import b64encode, b64decode
from uuid import UUID, uuid1
from ..base import Object, Entity

class ECDSA(Object):
    __slots__ = ('__sk', '__vk')
    
    def __init__(self, signing_key=None):
        libecdsa = __import__('ecdsa', fromlist=('SigningKey', 'NIST256p'))#SECP256k1
        libsha3 = __import__('sha3', fromlist=('sha3_256',))#keccak_256
        if isinstance(signing_key, bytes) and len(signing_key) == 32:
            sk = libecdsa.SigningKey.from_string(signing_key, libecdsa.NIST256p, libsha3.sha3_256)
        elif isinstance(signing_key, libecdsa.SigningKey):
            sk = signing_key
        else:
            sk = libecdsa.SigningKey.generate(curve=libecdsa.NIST256p, hashfunc=libsha3.sha3_256)
        self.__sk = sk
        self.__vk = sk.get_verifying_key()
    def sign(self, msg):
        return self.__sk.sign(msg)
    def verify(self, signature, msg):
        try:
            return self.__vk.verify(signature, msg)
        except:
            return False

class Token(Object):
    def generate(self, **kwargs):pass
    def verify(self, token):pass

class TokenECDSA(Object):
    __slots__ = ('__ecdsa',)
    
    def __init__(self, signing_key=None):
        self.__ecdsa = ECDSA(signing_key)
    def generate(self, **kwargs):
        nonce = uuid1().bytes
        payload = Entity.stringify(kwargs).encode()
        msg = b'%s%s' % (nonce, payload)
        signed = self.__ecdsa.sign(msg)
        return '%s.%s.%s' % (b64encode(nonce).decode(), b64encode(payload).decode(), b64encode(signed).decode())
    def verify(self, token):
        try:
            ts = token.split('.')
            if len(ts) == 3:
                nonce = b64decode(ts[0].encode())
                payload = b64decode(ts[1].encode())
                msg = b'%s%s' % (nonce, payload)
                signed = b64decode(ts[2].encode())
                if self.__ecdsa.verify(signed, msg):
                    return (True, str(UUID(bytes=nonce)).replace('-', ''), Entity.parse(payload.decode()))
        except:pass
        return (False, None, None)

class TokenJWT(Object):
    __slots__ = ('__serializer',)
    
    def __init__(self, skey=None, salt=None, skey_size=128, salt_size=32):
        its = __import__('itsdangerous', fromlist=('JSONWebSignatureSerializer', 'want_bytes'))
        libos = __import__('os', fromlist=('urandom',))
        self.__serializer = its.JSONWebSignatureSerializer(
            secret_key=skey if isinstance(skey, bytes) and len(skey) == skey_size else its.want_bytes(libos.urandom(skey_size)),
            salt=salt if isinstance(salt, bytes) and len(salt) == salt_size else its.want_bytes(libos.urandom(salt_size))
        )
    def generate(self, **kwargs):
        nonce = b64encode(uuid1().bytes).decode()
        return self.__serializer.dumps({'nonce': nonce, 'data': kwargs})
    def verify(self, token):
        try:
            data = self.__serializer.loads(token)
            if 'nonce' in data and 'data' in data:
                nonce = b64decode(data['nonce'].encode())
                return (True, str(UUID(bytes=nonce)).replace('-', ''), data['data'])
        except:pass
        return (False, None, None)
