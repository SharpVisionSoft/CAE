'''
Created on Oct 6, 2019

@author: liang.li
'''
from http import HTTPStatus
from mimetypes import add_type, guess_type
from os.path import splitext
from re import split as re_split
from uuid import getnode, uuid1
from ...io.fs import FS, DefaultFS
from .rs import Resource

class FileResource(Resource):
    __slots__ = ('__path', '__fs')
    
    __block_size = 8192
    __node_code = getnode()
    
    @classmethod
    def __parse_name(cls, a):
        b = re_split(b'Content-Disposition[ ]*:[ ]*form-data;[ ]*name[ ]*=[ ]*', a.rstrip())[1]
        c = re_split(b'[ ]*;[ ]*filename[ ]*=[ ]*', b)
        if len(c) > 1:
            f = c[1].decode()[1:-1]
            i = f.rfind('=')
            if i >= 0:
                f = f[i + 1:]
            return f
        return None
    
    def __init__(self, path, fs=None, **mtypes):
        self.__path = path
        self.__fs = fs if isinstance(fs, FS) else DefaultFS()
        for ext in mtypes:
            add_type(mtypes[ext], ext, strict=False)
    def __reader(self, path, size):
        bsize = self.__block_size
        loops = size // bsize + (0 if size % bsize == 0 else 1)
        with self.__fs.open(path) as f:
            try:
                for _ in range(loops):
                    yield f.read(bsize)
            except:pass
    def __create_files(self, path, data):
        try:
            ctype, clength, reader = data
            boundary = re_split('boundary[ ]*=[ ]*', ctype)
            if len(boundary) == 2:
                boundary = boundary[1].rstrip().encode()
                boundary = b'--%s' % boundary
                total = 0
                data = set()
                is_data = False
                fp = None
                last_ln = None
                while True:
                    ln = reader.readline()
                    if ln:
                        if ln.startswith(boundary):
                            if is_data:
                                if last_ln:
                                    fp.write(last_ln[:-2])
                                fp.close()
                            is_data = False
                            fp = None
                            last_ln = None
                            is_started = False
                        elif ln.startswith(b'Content-Disposition'):
                            filename = self.__parse_name(ln)
                            if filename:
                                ext = splitext(filename)
                                filename = '%s%s' % (str(uuid1(self.__node_code)).replace('-', ''), ext[1] or ext[0])
                                data.add(filename)
                                fp = self.__fs.open(self.__fs.join(path, filename), 'wb')
                        elif ln.startswith(b'Content-Type') or ln.startswith(b'Content-Transfer-Encoding'):
                            is_started = bool(fp)
                        elif ln == b'\r\n':
                            if is_started:
                                is_started = False
                                is_data = True
                            elif is_data:
                                if last_ln:
                                    fp.write(last_ln)
                                last_ln = ln
                        elif is_data:
                            if last_ln:
                                fp.write(last_ln)
                                fp.flush()
                            last_ln = ln
                        total += len(ln)
                        if total == clength:break
                    else:break
                return data
        except:pass
        return False
    def __listdir(self, path):
        data = []
        fs = self.__fs
        for x in fs.listdir(path):
            if not x.startswith('.'):
                fn = fs.join(path, x)
                isdir, mtime, size = fs.status(fn)
                data.append({'name': x, 'isdir': isdir, 'mtime': mtime, 'size': size})
        return data
    def __own_folder(self, payload):
        fs = self.__fs
        folder = '' if payload is None else payload.get('name', '')
        path = fs.join(self.__path, folder)
        if not fs.exists(path):
            fs.makedirs(path)
        return path, folder
        
    @Resource.route('/files', methods=['GET', 'POST'], need_login=True)
    def get_or_add_files(self, __request__):
        path, folder = self.__own_folder(__request__.payload)
        method = __request__.method
        if method == 'GET':
            data = self.__listdir(path)
            return self.response(data=data, total=len(data))
        if method == 'POST':
            data = self.__create_files(path, __request__.data)
            if data:
                return self.response(data=[{'folder': folder, 'name': x} for x in data], total=len(data))
            return self.response(HTTPStatus.BAD_REQUEST)
    @Resource.route('/files/*', methods=['GET', 'POST', 'DELETE'], need_login=True)
    def get_or_add_or_delete_files(self, __request__):
        fs = self.__fs
        p = __request__.path.replace('/files', '', 1).split('/')
        m = __request__.method
        if m == 'GET':
            path = fs.join(self.__path, *p)
            try:
                isdir, _, length = fs.status(path)
                if isdir:
                    data = self.__listdir(path)
                    return self.response(data=data, total=len(data))
                ctype = guess_type(path)[0]
                if ctype is None:
                    ctype = 'application/octet-stream'
                return (self.response_status(HTTPStatus.OK), (length, ctype, self.__reader(path, length)))
            except:pass
            return self.response(HTTPStatus.NOT_FOUND)
        path, folder = self.__own_folder(__request__.payload)
        path = fs.join(path, *p)
        if m == 'POST':
            if fs.exists(path):
                isdir, _, length = fs.status(path)
                if not isdir:
                    return self.response(HTTPStatus.BAD_REQUEST)
            else:
                fs.makedirs(path)
            data = self.__create_files(path, __request__.data)
            if isinstance(data, set):
                return self.response(data=[{'folder': fs.join(folder, *p), 'name': x} for x in data], total=len(data)) if data else self.response(HTTPStatus.BAD_REQUEST)
            return self.response()
        if m == 'DELETE':
            if fs.exists(path):
                fs.delete(path)
                return self.response()
            return self.response(HTTPStatus.NOT_FOUND)
