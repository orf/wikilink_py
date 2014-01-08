import csv


class CSVFormatter(object):
    def __init__(self, fd, headers, delimiter="\t"):
        self.headers = headers
        self.file = fd#csv.writer(fd, delimiter=delimiter)

    def add(self, output):
        self.file.write(output)
                #self.writer.writerow((o if isinstance(o, basestring) else o for o in row))

    def finished(self):
        pass

    def started(self):
        self.file.write(u"\t".join(self.headers) + "\n")
        #self.writer.writerow(self.headers)


class MultiCSVFormatter(object):
    def __init__(self, arg_iterable):
        self.writers = [CSVFormatter(fd,headers) for fd, headers in arg_iterable]

    def add(self, output):
        for count,val in enumerate(output):
            if val:
                self.writers[count].add(val)

    def finished(self):
        for writer in self.writers:
            writer.finished()

    def started(self):
        for writer in self.writers:
            writer.started()