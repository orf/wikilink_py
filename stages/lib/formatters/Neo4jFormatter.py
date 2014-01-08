from __pypy__.builders import UnicodeBuilder

BATCH_SIZE = 10


class Neo4jFormatter(object):
    def __init__(self, fd):
        self.builder = UnicodeBuilder()
        self.count = 0
        self.fd = fd

    def add(self, output):
        self.count += 1
        self.builder.append(output)
        if self.count == BATCH_SIZE:
            self.writeBatch()
            self.count = 0
            self.builder = UnicodeBuilder()
        else:
            self.builder.append(",")

    def writeBatch(self):
        built = self.builder.build()
        if built.endswith(","):
            built = built[:-1]
        self.fd.write("CREATE %s;\n" % built)

    def finished(self):
        self.writeBatch()

    def started(self):
        pass