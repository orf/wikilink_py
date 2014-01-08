from lib.split_brackets import split_brackets
from lib.progress import run_with_progressbar
import multiprocessing
import functools
import itertools
import contextlib
import operator
import os


def stage2(line, output_format):
    """
    I turn the latest-pagelinks.sql into a CSV of id|linktitle1|linktitle2|linktitle3...
    """

    if not line.startswith("INSERT INTO"):
        return None

    split_items = split_brackets(unicode(line, "utf-8", errors="ignore"))
    returner = []
    collector = []
    current_id = None

    for item in split_items:
        item = item.split("|")
        if current_id != item[0]:
            if collector:
                returner.append("%s|%s\n" % (current_id, "|".join(collector)))
                collector[:] = []

            current_id = item[0]
        #print item
        collector.append(item[2][1:-1])

    #for key, values_iter in itertools.groupby(split_items, operator.itemgetter(0)):
    #    returner.append(u"%s|%s\n" % (key, u"|".join([v[1] for v in list(values_iter)])))

    return u"".join(returner)


if __name__ == "__main__":
    # Run stage1
    with open('enwiki-latest-pagelinks.sql', "rb", buffering=1024*8) as input_fd: # , encoding="utf-8", buffering=1024*8
        with open('stage2.csv', mode="wb", buffering=1024*8) as output_fd:
            with contextlib.closing(multiprocessing.Pool(processes=4)) as tp:
                run_with_progressbar(input_fd, output_fd,
                                     functools.partial(stage2, output_format=None),
                                     os.path.getsize("enwiki-latest-pagelinks.sql"),
                                     iter_func=functools.partial(tp.imap_unordered))