"""python batchfetch.py /crawldata/urls5.txt prog.txt /crawldata/urls5.out 200 | tee /crawldata/urls5.log"""
import Queue
import bz2
import collections
import datetime
import os
import re
import sys
import time

import pycurl

import curl
import urls
import robots

##### Extract Command-line Args #########
USAGE = "Usage: %s <file with URLs to fetch> <progress_file> <output file> [<# of concurrent connections>]" % sys.argv[
    0]
try:
    [url_file, progress_file, out_file, parallel] = sys.argv[1:]
    parallel = int(parallel)
except:
    print USAGE
    raise SystemExit

##### Create Output File ########
if os.path.isfile(out_file):
    print "Output file already exists -- please delete first."
    raise SystemExit

if out_file.endswith(".bz2"):
    out_file = bz2.BZ2File(out_file, "w")
else:
    out_file = open(out_file, "w")

#################### UrlQueue ########################
# A class that provides a queue of urls, based on a given input list.
# It issues the urls in pseudo-fifo order, but with some tweaks:
# - you can ask it for another url from the same host
# This class has nothing to do with sleep-politeness.


class UrlScheduler:

    """This class has very interesting usage:
    1) insert_url() for a bunch of urls (of various domains)
    2) call next_url("")
      a) returns a url of a random "unused" domain; the domain is now used
    3) call next_url("domain")
      a) returns a url of that domain; the domain is now used (whether it was or not)
      b) calls next_url(""); also, "domain" is now "unused"
    """

    def __init__(self):
        self.domain_queues = {}       # for each domain, a Queue of urls
        # round-robin list (keys into domain_queues)
        self.domains = Queue.Queue()

        # map "domain" -> # times in-a-row next_url(domain) returned url for
        # that domain
        self.consecutive_count = {}
        self.num_urls = 0

        # domains that we don't emit urls for.
        ###self.locked_domains = set()

    def is_empty(self):
        # assmes empty queues disappear instantly
        return not self.domain_queues

    def size(self):
        return self.num_urls

    def Print(self, full=False):
        if full:
            for (domain, q) in self.domain_queues.iteritems():
                print domain, " -> ", q.qsize(),
                if domain in self.consecutive_count:
                    print self.consecutive_count[domain], " in-a-row"
                else:
                    print
        # print "locked: ", self.locked_domains
        print "UrlScheduler has", self.size(), "urls over", len(self.domain_queues), " domains"

    def insert_url(self, url, domain):
        urls = self.domain_queues.get(domain)
        if not urls:
            urls = Queue.Queue()
            self.domain_queues[domain] = urls
            self.domains.put(domain)

        urls.put(url)
        self.num_urls += 1

    def pop_url(self, domain):
        ##assert not domain in self.locked_domains

        urls = self.domain_queues.get(domain)
        if urls is None:
            return None

        # we already make sure empty lists never happen
        assert not urls.empty()
        self.consecutive_count[
            domain] = self.consecutive_count.get(domain, 0) + 1
        url = urls.get()
        self.num_urls -= 1

        # delete empty queues (self.domains gets cleaned up lazily)
        if urls.empty():
            del self.domain_queues[domain]
            del self.consecutive_count[domain]
        return url

    def next_url_new_domain(self, not_domains):
        """Return a url for a domain that hasn't been serviced lately."""
        num_domains = self.domains.qsize()
        for i in xrange(num_domains):
            domain = self.domains.get()

            # cleanup any domains that point to non-existant queues
            if not domain in self.domain_queues:
                continue  # without re-putting
            else:
                # put it back at the end for next round
                self.domains.put(domain)

            if domain in not_domains:
                continue
            # if domain in self.locked_domains: continue

            url = self.pop_url(domain)
            if url is None:
                continue

            return url
        return None

    def next_url(self, preferred_domain, not_domains):
        """Pop the next url for the preferred_domain.
        If that domain is empty (or we've gotten it too many times in a row),
        get a url from some other domain (that's not in not_domains)."""
        if not preferred_domain:
            return self.next_url_new_domain(not_domains)

        # if preferred_domain in self.locked_domains:
        # return self.next_url_new_domain(not_domains)

        if self.consecutive_count.get(preferred_domain, 0) > 10:
            del self.consecutive_count[preferred_domain]
            return self.next_url_new_domain(not_domains)

        url = self.pop_url(preferred_domain)
        if url is None:
            return self.next_url_new_domain(not_domains)

        return url

#################### Sleeper ###########################


class Sleeper(object):

    """Keep track of recent downloads for the sake of sleep/delay domain politeness.
    Uses a fixed amount of memory.
    """

    def __init__(self):
        # map "domain" -> time.time() when last fetch started.
        self.last_fetch_time = {}

        # Used to maintain a fixed memory size -- size of this queue is limited.
        # buckets that fall off queue are also removed from last_fetch_time.
        self.recent_buckets = Queue.Queue()

    def _sleep_bucket(self, url):
        return urls.getDomain(url)

    def delay_secs(self, url):
        """How long should the caller sleep before downloading this url?"""
        bucket = self._sleep_bucket(url)
        # 2 secs from last time.
        next_time = self.last_fetch_time.get(bucket, 0) + 2.0
        return max(0, next_time - time.time())

    def fetch_started(self, url):
        """We have to get both started and finished time because:
        a) we need to know it started to delay others from starting too
        b) we need to know it finished to delay *after* it finished.
        """
        self.fetch_finished(url)  # hack: just record the time the same way

    def fetch_finished(self, url):
        """Let's us know that a download has finished.  We need to know so that
        we can keep track of the last time when a domain got accessed."""
        bucket = self._sleep_bucket(url)
        if not self.last_fetch_time.has_key(bucket):
            self.recent_buckets.put(bucket)

        self.last_fetch_time[bucket] = time.time()
        self._limit_memory()

    def _limit_memory(self):
        while self.recent_buckets.qsize() > 10000:
            bucket = self.recent_buckets.get()
            del self.last_fetch_time[bucket]


######################## MrCrawlMulti ###############################
class MrCrawlMulti(curl.MultiFetcher):

    def __init__(self, parallel, output_file):
        super(MrCrawlMulti, self).__init__(parallel)
        self.useragent = "Mozilla/5.0 (compatible; MrCrawl/0.1; +http://mrcrawl.com/bot.html)"
        self.output_file = output_file
        self.url_scheduler = UrlScheduler()
        self.sleeper = Sleeper()
        self.robot_manager = robots.RobotsManager(
            self.useragent, "robots.db")

        self.curl_domains = set()  # len(curl_domains) <= len(self.curls)
        for curl in self.curls:
            curl.is_robots_fetch = False
            curl.last_domain = ""

    def handle_robots(self, curl):
        if not curl.is_robots_fetch:
            return False
        curl.is_robots_fetch = False

        domain = urls.getDomain(curl.url)
        host = urls.getHost(curl.url)
        try:
            robots_data = curl.output.getvalue().split("\r\n\r\n", 1)[1]
        except:
            robots_data = ""
            print "Robot Fetch had no content: ", curl.url

        self.robot_manager.update(host, robots_data)
        # self.url_scheduler.locked_domains.remove(domain)
        return True

    ctype_re = re.compile(r"(?i)\nContent-Type: .*\n")
    good_ctype_re = re.compile(r"text|html|charset")
    bad_ctype_re = re.compile(r"pdf")

    def should_save_page(self, response):
        # Find all "Content-Type:" headers in the first 1K bytes.
        # (There will be multiple if it's a redirect, because we have all responses.)
        ctype_lines = self.ctype_re.findall(response[:1000])
        if not ctype_lines:
            return True  # we don't know, err on the side of saving

        content_type = ctype_lines[-1].lower()
        if self.bad_ctype_re.search(content_type):
            return False
        if self.good_ctype_re.search(content_type):
            return True
        print content_type
        return False  # some other unknown type, drop it.

    def fetch_success(self, curl):
        effective_url = curl.getinfo(pycurl.EFFECTIVE_URL)
        now_str = datetime.datetime.utcnow().isoformat(" ")

        print "Success:", curl.url,
        if effective_url != curl.url:
            print effective_url,
        is_robots_txt = self.handle_robots(curl)
        self.sleeper.fetch_finished(curl.url)

        response = curl.output.getvalue()
        print "[", len(response), "bytes ]",

        if not is_robots_txt and self.should_save_page(response):
            self.output_file.write("\nmrcrawl_url: %s\n" % curl.url)
            self.output_file.write(
                "mrcrawl_effective_url: %s\n" % effective_url)
            self.output_file.write("mrcrawl_date: %s\n" % now_str)
            self.output_file.write(response)
            print "saved"
        else:
            print "dropped (bad content-type)"

    def fetch_failure(self, curl, errno, errmsg):
        print "Failed:", curl.url, errno, errmsg
        self.handle_robots(curl)
        self.sleeper.fetch_finished(curl.url)

    def fetch_roboted(self, url):
        print "Roboted:", url

    def fetch_filtered(self, url):
        print "Filtered:", url

    def do_some_work(self):
        last_domains = [
            curl.last_domain for curl in self.curls if curl.last_domain]
        if len(last_domains) != len(set(last_domains)):
            print last_domains
            assert False

        # For all the curls that aren't busy/sleeping, schedule a new url.
        for curl in self.curls:
            if curl.busy:
                continue
            if curl.sleeping:
                continue

            url = self.url_scheduler.next_url(
                preferred_domain=curl.last_domain, not_domains=self.curl_domains)
            if not url:
                continue
            domain = urls.getDomain(url)

            if not self.robot_manager.is_allowed(url):
                self.fetch_roboted(url)
                continue

            if self.robot_manager.needs_robot_fetch(url):
                # Put url back (and lock domain) while we fetch the robots file instead.
                # self.url_scheduler.locked_domains.add(domain)
                self.url_scheduler.insert_url(url, domain)
                url = self.robot_manager.robots_url(url)
                curl.is_robots_fetch = True

            curl.url = url
            if domain != curl.last_domain:
                if curl.last_domain:
                    self.curl_domains.remove(curl.last_domain)
                curl.last_domain = domain
                self.curl_domains.add(domain)
            curl.sleeping = True
            curl.sleeping_until = time.time() + self.sleeper.delay_secs(url)

        # For all the sleeping curls, see if it's time to wake up and start
        for curl in self.curls:
            if curl.sleeping and time.time() >= curl.sleeping_until:
                curl.sleeping = False
                curl.sleeping_until = 0
                self.sleeper.fetch_started(curl.url)
                # print "Starting url: ", curl.url
                self.start_url(curl, curl.url)

        super(MrCrawlMulti, self).do_some_work()

    url_filter_re = re.compile(r".*\.(pdf|ppt|ps)$")

    def should_crawl_url(self, url):
        if self.url_filter_re.match(url):
            return False
        return True

    def fetch_file(self, url_file):
        """Incrementatlly reads urls from url_file and feeds them into the system."""
        last_stat_time = int(time.time())
        start_time = time.time()

        file = open(url_file)
        eof = False
        while True:
            if eof and self.url_scheduler.is_empty() and self.num_curls_in_use == 0:
                break

            # feed the monster if he's hungry
            while not eof and self.url_scheduler.size() < 20000:
                line = file.readline()
                if line == "":
                    eof = True
                    break
                url = line.strip(" \n")
                if self.should_crawl_url(url):
                    domain = urls.getDomain(url)
                    self.url_scheduler.insert_url(url, domain)
                else:
                    self.fetch_filtered(url)

            # let the monster digest
            self.do_some_work()
            time.sleep(0.1)  # make sure we don't busy loop

            # print updates periodically
            if time.time() - last_stat_time > 20:
                last_stat_time = int(time.time())
                self.url_scheduler.Print()

########### Main Run #############
fetcher = MrCrawlMulti(parallel, out_file)
fetcher.fetch_file(url_file)
