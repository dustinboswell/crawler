import sys
import os
import datetime
import StringIO
import time
import bz2

import pycurl

print "PycURL %s (compiled against 0x%x)" % (pycurl.version, pycurl.COMPILE_LIBCURL_VERSION_NUM)

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass


# A "Curl" is a long-lived one-at-a-time fetcher.
def make_curl(useragent):
    c = pycurl.Curl()
    c.setopt(pycurl.USERAGENT, useragent)
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.MAXREDIRS, 5)
    c.setopt(pycurl.CONNECTTIMEOUT, 20)
    c.setopt(pycurl.TIMEOUT, 30)
    c.setopt(pycurl.NOSIGNAL, 1)
    c.setopt(pycurl.HEADER, 1)  # write out http response headers
    c.setopt(pycurl.ENCODING, "")  # "" -> Accept-encoding: gzip, zlib, ...
    #c.setopt(pycurl.VERBOSE , 1)
    return c

# Manages multiple Curl objects at once to get high-throughput parallel
# downloads.


class MultiFetcher(object):

    def __init__(self, parallel=2):
        self.curl_multi = pycurl.CurlMulti()
        self.curl_multi.handles = []
        self.curls = []
        self.num_curls_in_use = 0
        self.stats = {}
        self.useragent = "Mozilla/5.0"
        for i in xrange(parallel):
            c = make_curl(self.useragent)
            c.busy = False  # whether we are actively downloading
            c.sleeping = False
            c.sleeping_until = 0
            self.curl_multi.handles.append(c)
            self.curls.append(c)

    def start_url(self, curl, url):
        assert not curl.busy

        curl.busy = True
        curl.output = StringIO.StringIO()
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.WRITEFUNCTION, curl.output.write)
        self.curl_multi.add_handle(curl)
        self.num_curls_in_use += 1
        # store some extra info (not needed by pycurl)
        curl.url = url

    def add_count(self, key, inc):
        self.stats[key] = self.stats.get(key, 0) + inc

    def do_some_work(self):
        # sleep for a little bit to wait for data to be available.
        self.curl_multi.select(10.0)  # does this actually sleep??

        # Run the internal curl state machine for the multi stack
        perform_start = time.time()
        while True:
            if time.time() - perform_start > 0.1:
                break
            try:
                ret, num_handles = self.curl_multi.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break  # we might have some data ready
            except pycurl.error, e:
                # Unhandled exception: what do we do??
                print "Error code: ", e[0]
                print "Error message: ", e[1]

        # Check for curl objects which have terminated, and add them to the
        # freelist
        while True:
            num_q, ok_list, err_list = self.curl_multi.info_read()
            for curl in ok_list:
                self.fetch_success(curl)
                self.add_count("num_urls_success", 1)
                self.cleanup_curl(curl)

            for curl, errno, errmsg in err_list:
                self.fetch_failure(curl, errno, errmsg)
                self.add_count("num_urls_failure", 1)
                self.cleanup_curl(curl)

            # num_q > 0 means "info_read() has even more to give"
            if num_q == 0:
                break

    def cleanup_curl(self, curl):
        curl.output.close()
        curl.busy = False
        self.curl_multi.remove_handle(curl)
        self.num_curls_in_use -= 1

    # Callbacks for subclasses to override.
    # Warning: don't call any other CurlMulti method until these complete!
    def fetch_success(self, curl):
        pass

    def fetch_failure(self, curl, errno, errmsg):
        pass
