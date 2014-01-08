#from __pypy__.builders import UnicodeBuilder, StringBuilder
import multiprocessing
import multiprocessing.pool
import unicodecsv as csv
import progressbar
import os
import sys
import gzip
import contextlib
import argparse
import functools
import cStringIO
import itertools
import subprocess
import atexit
import time
import logging
import array
import cPickle
import json
import codecs

STAGE3_TITLES_TO_ID = {}
STAGE3_ID_TO_DATA = {}  # Data is (title, is_redirect (bool), has_seen (bool))


logger = multiprocessing.log_to_stderr(level=logging.INFO)


def load_dictionary(input_file):
    logger.info("Loading %s into memory" % input_file)
    with open(input_file, 'rb') as csv_fd:
        reader = csv.reader(csv_fd, delimiter="|", encoding="utf-8")
        for line in reader:
            # id title redirect (1/0)
            page_id = int(line[0])
            STAGE3_TITLES_TO_ID[line[1]] = page_id
            STAGE3_ID_TO_DATA[page_id] = (line[1], line[2] == "1", False)
    logger.info("Loaded %s page infos, secondary" % len(STAGE3_TITLES_TO_ID))
    return True


def get_ids_from_titles(titles_list, get_none=False):
    """
    I take a list of titles and return a list of integer ID's. If get_none is True then
    the return list will contain None values where the title cannot be found.
    """
    returner = []

    for title in titles_list:
        x = STAGE3_TITLES_TO_ID.get(title, None)
        if x is not None or get_none is True:
            returner.append(x)

    return returner


def get_page_data_from_id(page_id, update_seen=True):
    """
    I take a page ID and I return a tuple containing the title, is_redirect flag and a value indicating if this
    page ID has been queried before.
    """
    p_data = STAGE3_ID_TO_DATA.get(page_id, None)

    if p_data is None:
        return None

    if update_seen:
        STAGE3_ID_TO_DATA[page_id] = (p_data[0], p_data[1], True)

    return p_data


def set_page_redirect(title, to):
    """
    I replace a page title with the ID of the page it links to
    """
    STAGE3_TITLES_TO_ID[title] = to


def delete_page(title, id):
    """
    I take a page ID and I delete it from our registry
    """
    del STAGE3_TITLES_TO_ID[title]
    del STAGE3_ID_TO_DATA[id]


def split_brackets(line):
    start = None
    in_quotes = False

    num = 0
    line_len = len(line)
    b = line#buffer(line)
    ret = []

    while num + 1 <= line_len:
        char = b[num]

        if char == "\\":
            num += 1  # Skip one character

        elif char == "'":
            in_quotes = not in_quotes

        if not in_quotes:
            if char == "(":
                if start is None:
                    start = num

            elif char == ")":
                #yield line[start+1:num]
                #print start, num, line[start+1:num]
                ret.append(b[start+1:num])
                start = None

        num += 1
    print repr(ret[0])
    return ret


def run_with_progressbar2(input_fd, output_fd, line_func, output_format, output_func=None, iter_func=iter):
    #sys.stderr.write('Input: %s | Output: %s')
        output_writer = output_func(output_fd) if output_func else output_fd
        try:
            maxval = os.path.getsize(getattr(input_fd, "name", ""))
        except IOError:
            maxval = None

        pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ',
                                                progressbar.ETA(), '', progressbar.FileTransferSpeed()],
                                       maxval=maxval).start()

        start = time.time()

        for output in iter_func(functools.partial(line_func, output_format=output_format), input_fd):
            if output is not None:
                try:
                    output_writer.write(output.encode("utf-8"))
                except Exception:
                    raise

            if time.time() - start > 1:
                pbar.update(input_fd.tell())
                start = time.time()

        pbar.finish()


def stage1_writeheader(fobj):
    writer = csv.writer(fileobj=fobj, delimiter="|", encoding="utf-8")
    writer.writerow(("page_id", "title", "is_redirect"))
    return fobj


def stage1(line, output_format=None):
    """
    I create a CSV file that can be imported into
    """
    if not line.startswith("INSERT INTO"):
        return None
    res = csv.reader(split_brackets(unicode(line, "utf-8")), quotechar="'", escapechar="\\", doublequote=False, encoding="utf-8")
    out = cStringIO.StringIO()
    writer = csv.writer(out, delimiter="|", encoding="utf-8")
    for t in res:
        if t[1] == '0':
            writer.writerow((t[0], t[2], t[5]))
    return out.getvalue()


def stage2(line, output_format):
    """
    I turn the links SQL dump into a CSV of id|linktitle|linktitle|linktitle...
    This assumes that the input file (pagelinks.sql) is at least roughly in sequence, which I think it is (the inserts
    are in blocks, not mixed up)
    """

    if not line.startswith("INSERT INTO"):
        return None

    stage2_accumulator = []
    stage2_current_id = None
    stage2_returner = []
    res = csv.reader(split_brackets(unicode(line, "utf-8")), quotechar="'", doublequote=False, escapechar='\\', encoding="utf-8")
    for t in res:
        if stage2_current_id != t[0]:
            if stage2_accumulator:
                #print stage2_accumulator
                stage2_returner.append("%s|%s\n" % (stage2_current_id, "|".join(stage2_accumulator)))
                stage2_accumulator[:] = []

            stage2_current_id = t[0]
        stage2_accumulator.append(t[2])

    if stage2_accumulator:
        stage2_returner.append("%s|%s\n" % (stage2_current_id, "|".join(stage2_accumulator)))

    return u"".join(stage2_returner)#.build()


def split_page_info(line, update_seen=True, get_none=False):
    """
    I take a line outputted from Stage2 and I return (the_id, page_links, page_info)
    """
    page_links = line.rstrip("\n").split("|")
    page_id = int(page_links.pop(0))
    page_info = get_page_data_from_id(page_id, update_seen=update_seen)

    return page_id, get_ids_from_titles(page_links, get_none), page_info


def stage3_pre(line, output_format):
    page_id, page_links, page_info = split_page_info(line, update_seen=False, get_none=True)

    if page_info is None:
        return None

    page_title = page_info[0]

    if page_info[1]:  # Are we a redirect?
        if any(filter(lambda x: x is not None, page_links)) and page_links[0] is None:
            delete_page(page_title, page_id)
        else:
            set_page_redirect(page_title, page_links[0])


def stage3(line, output_format):
    """
    I combine the results from the previous stages into a single cohesive file
    """
    page_id, page_links, page_info = split_page_info(line)

    if page_info is None:  # Ignore redirects for now
        return None

    page_title, is_redirect, has_seen = page_info

    if not is_redirect:
        if has_seen is True:
            # Already visited this page before, output to an SQL file instead
            if output_format == "neo":
                pass
            else:
                with open('stage3.sql', 'a') as fd:
                    fd.write("UPDATE pages SET links = uniq(array_cat(links, ARRAY[%s]::integer[])) WHERE id = %s;\n" %
                             (",".join(map(str, set(page_links))), page_id))
        else:
            # CSV output
            # id, title, is_redirect, links_array
            if output_format == "neo":
                return "CREATE ({id:%s, name:%s});\n" % (page_id, json.dumps(page_title))
            else:
                return "%s|%s|%s|{%s}\n" % (page_id, page_title, is_redirect,
                                            ",".join(map(str, set(page_links))))


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    method_group = args.add_mutually_exclusive_group(required=True)
    method_group.add_argument("--ids", action="store_true",
                              help="Parse latest-page SQL file into an importable CSV file")
    method_group.add_argument("--links", action="store_true",
                              help="Parse pagelinks SQL file into an intermediate format, suitable for importing via this tool")
    method_group.add_argument("--inputlinks", action="store_true",
                              help="Parse --links output and import it into the database")
    output_group = args.add_argument_group("output and input")
    output_group.add_argument("--input", type=argparse.FileType('rb', bufsize=1024*8), help="Input file to use", required=True)
    output_group.add_argument("--output", type=argparse.FileType('wb', bufsize=1024*8), help="Output file to use", required=True)
    output_group.add_argument("--outgzip", action="store_true", help="Use Gzip to compress output (this flag is slower than piping stdout through gzip)")
    output_group.add_argument("--ingzip", action="store_true", help="Use Gzip to decompress input file/stream (this flag is slower than piping file through gzip)")
    output_group.add_argument("--titles_csv", action="store", help="Path to latest-page parsed CSV file (required for --inputlinks)")
    output_format_grpup = output_group.add_mutually_exclusive_group()
    output_format_grpup.add_argument("--sqloutput", dest="output_format", const="sql", default="sql",
                                     action="store_const", help="Write output suitable for importing into PostgreSQL")
    output_format_grpup.add_argument("--neo4joutput", dest="output_format", const="neo",
                                     action="store_const", help="Write output suitable for importing into neo4j")

    concurrency_group = args.add_argument_group("concurrency")
    pool_group = concurrency_group.add_mutually_exclusive_group()
    pool_group.add_argument("--processpool", action="store_const", dest="pool",
                            const=multiprocessing.Pool, default=multiprocessing.Pool)
    pool_group.add_argument("--threadpool", action="store_const", dest="pool",
                            const=multiprocessing.pool.ThreadPool)
    pool_group.add_argument("--nopool", action="store_const", dest="pool", const=None)

    concurrency_group.add_argument("--workers", action="store", type=int, default=0)
    command = args.parse_args()

    if command.outgzip:
        command.output = gzip.GzipFile(fileobj=command.output)

    if command.ingzip:
        command.input = gzip.GzipFile(fileobj=command.input)

    runner = functools.partial(run_with_progressbar, command.input, command.output, output_format=command.output_format)

    func_to_run = None

    if command.ids:
        func_to_run = stage1
        #runner = functools.partial(runner, output_func=stage1_writeheader)
    elif command.links:
        func_to_run = stage2
    elif command.inputlinks:
        if not command.titles_csv:
            raise Exception("You must use --titles_csv")
        command.pool = None
        load_dictionary(command.titles_csv)
        func_to_run = [stage3_pre, stage3]

    if func_to_run:
        if not isinstance(func_to_run, list):
            func_to_run = [func_to_run]

        #codecs.getreader()

        with contextlib.closing(command.input) as input_fd:
            with contextlib.closing(command.output) as output_fd:
                for f in func_to_run:
                    command.input.seek(0)

                    if command.pool is None:
                        runner(f, iter_func=itertools.imap)
                    else:
                        with contextlib.closing(command.pool(processes=command.workers or None)) as tp:
                            print "Running %s with %s, input %s output %s, format %s" % (f, command.pool, command.input,
                                                                                         command.output, command.output_format)
                            runner(f, iter_func=functools.partial(tp.imap_unordered, chunksize=8))

    #COPY pages(id,title,is_redirect,links) FROM 'C://Users//tom//PycharmProjects//wikilink//stage3.csv' DELIMITER '|' QUOTE '#' CSV;
    """UPDATE pages redirect
 SET links = non_redirect.links + redirect.links
  FROM pages non_redirect
   WHERE redirect.is_redirect=TRUE
   AND icount(redirect.links) = 1
   AND non_redirect.id = redirect.links[1]"""