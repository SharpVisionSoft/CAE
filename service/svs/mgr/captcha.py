'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from base64 import b64encode
from uuid import uuid1
from ..base import Object
from ..io.cache import MemoryCache, ThreadingMemoryCache

class Captcha(Object):
    __slots__ = ('__cache', '__font')
    
    def __init__(self, cache, font='arial.ttf'):
        self.__cache = cache if isinstance(cache, MemoryCache) else ThreadingMemoryCache()
        self.__font = font
    def _generate(self, size, **kwargs):pass
    def generate(self, size=5, **kwargs):
        code, raw = self._generate(size, font=self.__font, **kwargs)
        identity = str(uuid1()).replace('-', '')
        self.__cache.put(identity, code.strip().lower(), 300)
        return (identity, raw)
    def verify(self, identity, code):
        return self.__cache.pop(identity) == str(code).lower()

class GraphicVerification(Captcha):
    __pil = None
    __random = None
    __io = None
    __LOOKUP_TABLE = (
        '346789AaBCcDdEeFfGHJjKkLMmNnPpRSsTtUuVvWwXxYyZz',
        'data:image/%s;base64,%s'
    )
    
    @classmethod
    def __import_libs(cls):
        if cls.__pil is None:
            cls.__pil = __import__('PIL', fromlist=('Image', 'ImageDraw', 'ImageFilter', 'ImageFont'))
            cls.__random = __import__('random', fromlist=('sample', 'randint'))
            cls.__io = __import__('io', fromlist=('BytesIO',))
            
    def __init__(self, cache, font='arial.ttf'):
        self.__import_libs()
        super().__init__(cache, font)
    def _generate(self,
        size=5,
        img_size=(136, 32),
        img_type='png',
        img_text_color=(255, 0, 0),
        img_bg_color=(255, 255, 255),
        img_fg_color=(255, 255, 0),
        drawing_lines=True,
        drawing_points=True,
        font='arial.ttf'
    ):
        pil = self.__pil
        ran = self.__random
        randint = ran.randint
        
        code = ' %s ' % ''.join(ran.sample(self.__LOOKUP_TABLE[0], size))
        font = pil.ImageFont.truetype(font, size=24)
        fw, _ = font.getsize(code)
        w, h = img_size
        img = pil.Image.new('RGB', img_size, img_bg_color)
        img_draw = pil.ImageDraw.Draw(img)
        img_draw.text((int((w - fw) / 3.5), 0), code, font=font, fill=img_text_color)
        if drawing_lines:
            for _ in range(randint(3, 5)):
                img_draw.line([(randint(0, w), randint(0, h)), (randint(0, w), randint(0, h))], fill=img_fg_color)
        if drawing_points:
            for w_ in range(w):
                for h_ in range(h):
                    if randint(0, 100) > 97: #drawing 3%
                        img_draw.point((w_, h_), fill=img_fg_color)
        data = [
            1.0 - float(randint(1, 2)) / 100.0,
            0.0,
            0.0,
            0.0,
            1.0 - float(randint(1, 10)) / 100.0,
            float(randint(1, 2)) / 500.0,
            0.001,
            float(randint(1, 2)) / 500.0
        ]
        img = img.transform(img_size, pil.Image.PERSPECTIVE, data=data)
        img = img.filter(pil.ImageFilter.EDGE_ENHANCE_MORE)
        with self.__io.BytesIO() as bytesIO:
            img.save(bytesIO, img_type)
            img = bytesIO.getvalue()
        img = b64encode(img)
        return (code, self.__LOOKUP_TABLE[1] % (img_type, img.decode()))
