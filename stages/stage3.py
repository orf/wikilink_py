from lib.progress import run_with_progressbar
from lib.formatters.Neo4jFormatter import Neo4jFormatter
from lib.formatters.CSVFormatter import MultiCSVFormatter
import functools
import os
import logging
import sys
import itertools
import __pypy__
import json

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

STAGE3_TITLES_TO_ID = {}
STAGE3_ID_TO_DATA = {}


FLAG_REDIRECT = 1
FLAG_SEEN = 2


def handle_stage1_line(line):
    # There is one page in stage1.csv who's title is a unicode NEXT_LINE character (\x85).
    # As such we have to encode each line individually.
    # https://en.wikipedia.org/w/api.php?action=query&prop=info&pageids=28644448&inprop=url
    page_id, page_title, is_redirect = unicode(line.strip("\n"), "utf-8").split("|")
    flags = FLAG_REDIRECT if is_redirect == "1" else 0

    STAGE3_TITLES_TO_ID[page_title] = int(page_id)
    STAGE3_ID_TO_DATA[int(page_id)] = (page_title, flags)


    #yield (page_title, flags), int(page_id)


def get_ids_from_titles(titles_list, get_none=False):
    """
    I take a list of titles and return a list of integer ID's. If get_none is True then
    the return list will contain None values where the title cannot be found.
    """
    returner = []

    for title in titles_list:
        x = STAGE3_TITLES_TO_ID.get(title, 0)
        if x is not 0 or get_none is True:
            returner.append(x)  # Keeping all elements uniform might increase performance

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
        STAGE3_ID_TO_DATA[page_id] = (p_data[0], p_data[1] | FLAG_SEEN)

    return p_data


def set_page_redirect(title, to):
    """
    I replace a page title with the ID of the page it links to
    """
    STAGE3_TITLES_TO_ID[title] = to


def delete_page(title, page_id):
    """
    I take a page ID and/or I delete it from our registry
    """
    if title:
        del STAGE3_TITLES_TO_ID[title]

    if page_id:
        del STAGE3_ID_TO_DATA[page_id]


def split_page_info(line, update_seen=True, get_none=False, get_links=True):
    """
    I take a line outputted from Stage2 and I return (the_id, page_links, page_info)
    """
    line = line.rstrip("\n")
    split_line = line.split("|")
    page_id = int(split_line[0])
    page_info = get_page_data_from_id(page_id, update_seen=update_seen)

    if page_info is None:
        return None, None, None

    # Using islice like this keeps memory down by avoiding creating another list, it also doens't need a len() call
    # so it might be faster. whatever.
    page_links = itertools.islice(split_line, 1, sys.maxint)

    return page_id, get_ids_from_titles(page_links, get_none) if get_links else page_links, page_info


def stage3_pre(line):
    """
    We need to sort out redirects so they point to the correct pages. We do this by
    loading stage2.csv which contains ID|link_title|link_title... and get the ID's of the links
    """
    page_id, page_links, page_info = split_page_info(unicode(line, "utf-8"), update_seen=False, get_links=False)

    if page_info and page_info[1] & FLAG_REDIRECT:  # Are we a redirect?
        page_links = get_ids_from_titles(page_links, True)

        page_title = page_info[0]
        if len(page_links) > 1 and page_links[0]:
            # Point the redirect page to the ID of the page it redirects to
            set_page_redirect(page_title, page_links[0])
            delete_page(None, page_id)
        else:
            # The page we are redirecting to cannot be found, remove the redirect page.
            delete_page(page_title, page_id)


def stage3(line, output_format="neo"):
    """
    I combine the results from the previous stages into a single cohesive file
    """
    global STAGE3_ROW_COUNTER

    page_id, page_links, page_info = split_page_info(unicode(line.strip("\n"), "utf-8"), get_links=False)

    if page_info is None:  # Ignore redirects for now
        return None

    page_title, flags = page_info
    #print "flags: %s" % flags

    if not flags & FLAG_REDIRECT:
        page_links = get_ids_from_titles(page_links, False)

        if flags & FLAG_SEEN:
            # Already visited this page before, output to an SQL file instead
            if output_format == "neo":
                return None, "\n".join(["%s\t%s" % (page_id, link_id) for link_id in set(page_links)])
            else:
                with open('stage3.sql', 'a') as fd:
                    fd.write("UPDATE pages SET links = uniq(array_cat(links, ARRAY[%s]::integer[])) WHERE id = %s;\n" %
                             (",".join(map(str, set(page_links))), page_id))
        else:
            # CSV output
            # id, title, is_redirect, links_array
            if output_format == "neo":
                #return u"({id:%s, name:%s})" % (page_id, json.dumps(page_title).encode("unicode-escape"))
                return ("%s\t%s\n" % (page_id, page_title)).encode("utf-8"),\
                        "%s\n" % "\n".join(["%s\t%s" % (page_id, link_id) for link_id in set(page_links)])
                #return ((page_id, page_title),),
            else:
                return "%s|%s|%s|{%s}\n" % (page_id, page_title, is_redirect,
                                            ",".join(map(str, set(page_links))))



if __name__ == "__main__":
    logger.info("Loading stage1.csv into memory")
    with open("stage1.csv", 'rb', buffering=1024*1024) as csv_fd:
        run_with_progressbar(csv_fd, None, handle_stage1_line, os.path.getsize("stage1.csv"))

    logger.info("Loaded %s/%s page infos. Strategies: %s and %s" % (len(STAGE3_TITLES_TO_ID), len(STAGE3_ID_TO_DATA),
                                                                    __pypy__.dictstrategy(STAGE3_ID_TO_DATA),
                                                                    __pypy__.dictstrategy(STAGE3_TITLES_TO_ID)))

    with open("stage2.csv", "rb", buffering=1024*1024) as input_fd:
        run_with_progressbar(input_fd, None, stage3_pre, os.path.getsize("stage2.csv"))

    logger.info("Have %s/%s page infos. Strategies: %s and %s" % (len(STAGE3_TITLES_TO_ID), len(STAGE3_ID_TO_DATA),
                                                                __pypy__.dictstrategy(STAGE3_ID_TO_DATA),
                                                                __pypy__.dictstrategy(STAGE3_TITLES_TO_ID)))

    logger.info("Starting dump")
    with open('stage2.csv', "rb", buffering=1024*1024*8) as input_fd: # , encoding="utf-8", buffering=1024*8
        with open('stage3.nodes', mode="wb", buffering=1024*1024*8) as nodes_fd:
            with open('stage3.links', mode="wb", buffering=1024*1024*20) as links_fd:

                formatter = MultiCSVFormatter(((nodes_fd, ("id:int:node_id", "title:string")),
                                               (links_fd, ("id:int:node_id", "id:int:node_id"))))

                run_with_progressbar(input_fd, None,
                                     functools.partial(stage3, output_format="neo"),
                                     os.path.getsize("stage2.csv"),
                                     formatter=formatter)