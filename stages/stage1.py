from lib.split_brackets import split_brackets
from lib.builders import UnicodeStringBuilder
from lib.progress import run_with_progressbar
import itertools
import sys
import os
import functools
import contextlib
import multiprocessing


def stage1(line):
    """
    I turn the latest-page.sql into a CSV containing rows of (id, title, is_redirect) delimited by |
    """
    if not line.startswith("INSERT INTO"):
        return None

    split = split_brackets(unicode(line, "utf-8"))
    output_writer = UnicodeStringBuilder()

    for output in split:
        output = output.split("|")
        if output[1] == "0":  # If the namespace is 0 its a 'normal' page
            output_writer.write(u"%s|%s|%s\n" % (output[0], output[2][1:-1], output[5]))

    return output_writer.getValue()


def run_prog():
    pass


if __name__ == "__main__":
    # Run stage1
    with open('enwiki-latest-page.sql', "rb", buffering=1024*8) as input_fd: # , encoding="utf-8", buffering=1024*8
        with open('stage1.csv', mode="wb", buffering=1024*8) as output_fd:
            with contextlib.closing(multiprocessing.Pool()) as tp:
                ifunc = functools.partial(tp.imap_unordered, chunksize=8)
                run_with_progressbar(input_fd, output_fd,
                                     functools.partial(stage1),
                                     os.path.getsize("enwiki-latest-page.sql"),
                                     iter_func=ifunc)