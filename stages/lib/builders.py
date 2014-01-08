from __pypy__.builders import UnicodeBuilder


class UnicodeStringBuilder(object):
    def __init__(self):
        self.b = UnicodeBuilder()

    def getValue(self):
        return self.b.build()

    def append(self, val):
        self.b.append(val)

    def write(self, val):
        self.b.append(val)

    def write_slice(self, *args, **kwargs):
        self.b.append_slice(*args, **kwargs)