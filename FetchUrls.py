import urllib
import re


class AppURLopener(urllib.FancyURLopener):
    version = "Mozilla/2.0 (compatible; AOL 3.0; Mac_PowerPC)"

urllib._urlopener = AppURLopener()

urls = []


def GetResultUrls(query):
    query = urllib.quote(query)
    html = urllib.urlopen(
        "http://www.google.com/search?q=%s&num=100" % query).read()
    lines = html.split('"')
    for line in lines:
        if re.match("http://[^0-9g].*", line) and line.find("google") == -1:
            print line
            urls.append(line)

for query in open("/usr/share/dict/propernames").readlines():
    query = query.strip("\n")
    # GetResultUrls(query)
    query_rev = "".join([query[-i] for i in xrange(0, len(query))])
    GetResultUrls(query_rev)
