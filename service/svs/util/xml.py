'''
Created on Oct 12, 2019
@author: Liang.Li
'''
from io import StringIO
from xml.sax import ContentHandler, make_parser, handler
from ..base import Object, Entity
from .ds import Stack

class XMLTag(Entity):
    __slots__ = ('name', 'attrs', 'text', 'children')
        
    def __init__(self, name=None, attrs=None, text=None, children=None):
        self.name = name
        self.attrs = attrs if isinstance(attrs, dict) else {}
        self.text = text
        self.children = list(children) if isinstance(children, (list, tuple)) else []
    def find(self, path, leaves=None):
        if isinstance(path, str):
            names = [name.strip() for name in path.split('.')]
            if names:
                if names[0] == self.name:
                    names = names[1:]
                founds = [self]
                if names:
                    current_children = self.children
                    for name in names:
                        founds.clear()
                        if current_children:
                            found_children = []
                            for child in current_children:
                                if name == child.name:
                                    founds.append(child)
                                    for c in child.children:
                                        found_children.append(c)
                            current_children = found_children
                        else:
                            break
            if leaves is None:
                for found in founds:
                    yield found
            elif founds:
                if isinstance(leaves, str):
                    for leaf in leaves.split(','):
                        for found in founds:
                            for child in found.children:
                                if leaf.strip() == child.name:
                                    yield child
    def get_first(self, path, leaves=None):
        findings = self.find(path, leaves)
        for finding in findings:
            findings.close()
            return finding
        return None
    def stringify_attributes(self):
        with StringIO() as writer:
            for k, v in self.attrs.items():
                writer.write(' %s="%s"' % (k, v))
            return writer.getvalue()

class XMLReader(Object):
    __slots__ = ('__contentHandler', '__path')
    
    class __ContentHandler(ContentHandler):
        def __init__(self):
            super().__init__()
            self.stack = Stack()
            self.content = []
        def startElement(self, tag, attributes):
            e = XMLTag(name=tag, attrs={k:v for k, v in attributes.items()})
            p = self.stack.peek()
            if p is not None:
                p.children.append(e)
            self.stack.push(e)
        def endElement(self, tag):
            e = self.stack.pop()
            if e.name != tag:
                raise 'Invalid XML File'
            if self.stack.is_empty:
                self.content.append(e)
        def characters(self, content):
            text = content.strip()
            if text:
                e = self.stack.peek()
                e.text = text
        def clear(self):
            self.content.clear()
            self.stack.clear()
        def data(self):
            return self.content[0] if self.content else XMLTag()
    
    def __init__(self, path):
        self.__path = path
        self.__contentHandler = self.__ContentHandler()
    def read(self):
        contentHandler = self.__contentHandler
        try:
            parser = make_parser()
            parser.setFeature(handler.feature_namespaces, 0)
            parser.setContentHandler(contentHandler)
            parser.parse(self.__path)
            return contentHandler.data()
        finally:
            contentHandler.clear()
    @property
    def path(self):
        return self.__path

class XMLWriter(Object):
    __slots__ = ('__path',)
    
    def __init__(self, path):
        self.__path = path
    def __write(self, writer, tag, seq):
        name = tag.name
        if name != None:
            name = name.encode()
            tab = b'\t' * seq
            writer.writelines([
                tab,
                b'<%s%s>' % (name, tag.stringify_attributes().encode())
            ])
            text = tag.text
            if text != None:
                writer.write(text.encode())
            children = tag.children
            if children:
                writer.write(b'\n')
                seq += 1
                for child in children:
                    self.__write(writer, child, seq)
                writer.write(tab)
            writer.writelines([b'</%s>' % name, b'\n'])
            writer.flush()
    def write(self, tag):
        if isinstance(tag, XMLTag):
            with open(self.__path, 'wb') as writer:
                writer.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
                self.__write(writer, tag, 0)                        
            return True
        return False
    @property
    def path(self):
        return self.__path
