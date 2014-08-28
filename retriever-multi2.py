#! /usr/bin/env python
# Usage: python retriever-multi.py <file with URLs to fetch> [<# of
#          concurrent connections>]

import sys
import os
import datetime
import cStringIO
import StringIO
import bz2

import pycurl

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass


# Get args
num_conn = 10
try:
    if sys.argv[1] == "-":
        urls = sys.stdin.readlines()
    else:
        urls = open(sys.argv[1]).readlines()
        output_filename = sys.argv[2]
        if os.path.isfile(output_filename):
            print "Output file already exists -- please delete first."
            raise SystemExit

        if output_filename.endswith(".bz2"):
            output_file = bz2.BZ2File(output_filename, "w")
        else:
            output_file = open(output_filename, "w")

    if len(sys.argv) >= 4:
        num_conn = int(sys.argv[3])
except:
    print "Usage: %s <file with URLs to fetch> <output file> [<# of concurrent connections>]" % sys.argv[0]
    raise SystemExit


# Make a queue with url strings
queue = []
for url in urls:
    # TODO: do we need to remove #anchor; filter bad urls types; remove dups
    url = url.strip()
    if not url or url[0] == "#":
        continue
    queue.append(url)


# Check args
assert queue, "no URLs given"
num_urls = len(queue)
num_conn = min(num_conn, num_urls)
assert 1 <= num_conn <= 10000, "invalid number of concurrent connections"
print "PycURL %s (compiled against 0x%x)" % (pycurl.version, pycurl.COMPILE_LIBCURL_VERSION_NUM)
print "----- Getting", num_urls, "URLs using", num_conn, "connections -----"


# Pre-allocate a list of curl objects
m = pycurl.CurlMulti()
m.handles = []
for i in range(num_conn):
    c = pycurl.Curl()
    c.output = None
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.MAXREDIRS, 5)
    c.setopt(pycurl.CONNECTTIMEOUT, 10)
    c.setopt(pycurl.TIMEOUT, 30)
    c.setopt(pycurl.NOSIGNAL, 1)
    c.setopt(pycurl.HEADER, 1)  # write out http response headers
    c.setopt(pycurl.ENCODING, "")  # "" -> Accept-encoding: gzip, zlib, ...
    #c.setopt(pycurl.VERBOSE , 1)
    m.handles.append(c)


# Main loop
freelist = m.handles[:]
num_processed = 0
while num_processed < num_urls:
    # If there is an url to process and a free curl object, add to multi stack
    while queue and freelist:
        url = queue.pop(0)
        c = freelist.pop()
        c.output = StringIO.StringIO()
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.WRITEFUNCTION, c.output.write)
        m.add_handle(c)
        # store some info
        c.url = url
    # Run the internal curl state machine for the multi stack
    while 1:
        try:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        except pycurl.error, e:
            # Unhandled exception: what do we do??
            print "Error code: ", e[0]
            print "Error message: ", e[1]

    # Check for curl objects which have terminated, and add them to the
    # freelist
    while 1:
        num_q, ok_list, err_list = m.info_read()
        for c in ok_list:
            effective_url = c.getinfo(pycurl.EFFECTIVE_URL)
            output_file.write("\nmrcrawl_url: %s\n" % c.url)
            output_file.write("mrcrawl_effective_url: %s\n" % effective_url)
            output_file.write("mrcrawl_date: %s\n" %
                              datetime.datetime.utcnow().isoformat(" "))
            output_file.write(c.output.getvalue())
            c.output.close()
            m.remove_handle(c)
            print "Success:", c.url, c.getinfo(pycurl.EFFECTIVE_URL)
            freelist.append(c)
        for c, errno, errmsg in err_list:
            c.output.close()
            m.remove_handle(c)
            print "Failed: ", c.url, errno, errmsg
            freelist.append(c)
        num_processed = num_processed + len(ok_list) + len(err_list)
        if num_q == 0:
            break
    # Currently no more I/O is pending, could do something in the meantime
    # (display a progress bar, etc.).
    # We just call select() to sleep until some more data is available.
    m.select(1.0)


# Cleanup
output_file.close()
for c in m.handles:
    c.close()
m.close()
