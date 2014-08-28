usage = """
Example uses:
./wget.py --output-prefix=/foo/boo url1s.txt urls2.txt
    Will produce the following files:
    - /foo/boo1.out [all html, headers, urls]
    - /foo/boo1.log [all debug output]
    - /foo/boo2.out
    - /foo/boo2.log
    - ...

./wget.py --wget-args="--depth=1" ...
    Same as above, but adds/overrides any wget args
"""

from threading import Thread
import os
import sys
import getopt
from Queue import Queue

# Flags and Defaults
arg_dict = {}
arg_dict["output-prefix"] = "./"
arg_dict["wget-args"] = ""
arg_dict["max-threads"] = "4"
args = []  # the trailing args with no "--"

if len(sys.argv) <= 1:
    print usage
    sys.exit(2)

# Extract flags from sys.argv
try:
    opts, args = getopt.getopt(
        sys.argv[1:], '', ["output-prefix=", "wget-args=", "max-threads="])
except getopt.GetoptError, err:
    # print help information and exit:
    print str(err)  # will print something like "option -a not recognized"
    sys.exit(2)
for option, value in opts:
    arg_dict[option.strip("-")] = value


BASE_PARAMS = {
    #'user-agent': "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_8; en-US) AppleWebKit/533.2 (KHTML, like Gecko) Chrome/5.0.342.9 Safari/533.2",
    'dns-timeout': '10.0',
    'connect-timeout': '10.0',
    'read-timeout': '30.0',
    # 'timestamping': None,  # does HEAD request if we already have this file, checks Last-Modified before re-downloading
    'tries': '2',
    'wait': '5.0',  # secs/request
    'random-wait': None,  # vary up the wait a little
    'debug': None,
    # 'directory-prefix': '/crawldata',
    'follow-tags': 'a',  # But only follow <a href>, no others
}

DOMAIN_SPIDER_PARAMS = BASE_PARAMS.copy()
DOMAIN_SPIDER_PARAMS.update({
    'recursive': None,   # Follow links
    'follow-tags': 'a',  # But only follow <a href>, no others
    'span-hosts': None,  # Follow links to different hosts
    # But only stay within these domains (You must override this)
    'domains': '',
    'level': '8',        # Recursion depth (0 means "don't follow any links")
})


def arg_dict_str(d):
    s = ''
    for key, value in d.iteritems():
        s += ' --%s' % key
        if value is not None:
            s += '="%s"' % value
    return s


def exec_cmd(cline):
    print cline
    retval = os.system(cline)
    if retval != 0:
        print "Command failed: ", cline


def WgetFile(fname, shard_id):
    params = BASE_PARAMS.copy()
    params['output-file'] = "%s%d.log" % (arg_dict["output-prefix"], shard_id)
    #params['output-document'] = "%s%d.out" % (arg_dict["output-prefix"], shard_id)
    params['output-document'] = "-"
    params['server-response'] = None
    params['save-headers'] = None
    params['save-url'] = None

    cline = "wget %s %s -i %s" % (arg_dict_str(params),
                                  arg_dict["wget-args"], fname)
    cline += " >> %s%d.out" % (arg_dict["output-prefix"], shard_id)
    exec_cmd(cline)

    cline = "bzip2 %s" % params['output-document']
    exec_cmd(cline)


def WgetDomain(domain, shard_id):
    params = DOMAIN_SPIDER_PARAMS.copy()
    params['output-file'] = "%s/.wget.log" % domain
    params['domains'] = domain

    os.system("mkdir -p %s" % domain)  # needed so .wget.log can be made

    cline = "wget " + arg_dict_str(params) + " http://%s" % domain
    print "Starting crawl for %s ..." % domain
    print cline
    os.system(cline)
    print "Finished with crawl for %s" % domain


def WgetParallel(func, args):
    q = Queue()

    def Worker():
        while True:
            (shard_id, arg) = q.get()
            func(arg, shard_id)
            q.task_done()

    num_threads = min(int(arg_dict["max-threads"]), len(args))
    for i in xrange(num_threads):
        t = Thread(target=Worker)
        t.setDaemon(False)
        t.start()

    for i in xrange(len(args)):
        q.put((i, args[i]))

    q.join()   # block until all tasks are done

    print "Done with all wget tasks. Hit Ctrl-C to stop"

    # Dumb code to wait for Ctrl-C
    while True:
        if os.system("sleep 2") != 0:  # returns non-0 if user did Ctrl-C
            sys.exit(-1)

# WgetParallel(WgetDomain, sys.argv[1:])
# WgetFile(sys.argv[1])
WgetParallel(WgetFile, args)
