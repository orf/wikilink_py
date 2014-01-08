import functools
import time
import progressbar
import itertools
import codecs


def run_with_progressbar(input_fd, output_fd, line_func, maxval=None,
                         iter_func=itertools.imap, output_func=None, formatter=None):
    #sys.stderr.write('Input: %s | Output: %s')
        pbar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ', progressbar.Bar(), ' ',
                                                progressbar.ETA(), '', progressbar.FileTransferSpeed()],
                                       maxval=maxval).start()

        start = time.time()
        ticker = 0

        if formatter:
            formatter.started()

        for output in iter_func(line_func, input_fd):
            if output is not None:
                if output_func:
                    output_func(output_fd, output.encode("utf-8"))
                elif formatter:
                    formatter.add(output)
                else:
                    output_fd.write(output.encode("utf-8"))

            # Calling time.time() thousands of times a second is kind of slow.
            # We only check the time every 2000 lines
            ticker += 1
            if ticker == 2000:
                if time.time() - start > 1:
                    pbar.update(input_fd.tell())
                    start = time.time()
                ticker = 0

        if formatter:
            formatter.finished()

        pbar.finish()