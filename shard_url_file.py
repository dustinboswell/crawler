"""Example:   python shard_url_file.py urls.txt 3
Creates files:
    urls.txt.0
    urls.txt.1
    urls.txt.2
where file i only contains urls for which
    hash(domain(url)) % 3 == i

Also, removes duplicate urls.
Also, filters out pdf/xls/other url types
"""

import urllib
import sys
import urls


def skip_url(url):
    url = url.lower()
    if url.endswith(".pdf"):
        return True
    if url.endswith(".ps"):
        return True
    if url.endswith(".ppt"):
        return True
    if url.endswith(".xls"):
        return True
    if url.endswith(".doc"):
        return True
    if url.endswith(".zip"):
        return True
    return False

num_shards = int(sys.argv[2])
out_files = [open(sys.argv[1] + "." + str(i), "w") for i in xrange(num_shards)]
seen_urls = set()

for line in open(sys.argv[1]).readlines():
    url = line.strip(" \n\r")
    domain = urls.getDomain(url)
    shard = hash(domain) % num_shards

    if skip_url(url):
        continue

    if url not in seen_urls:
        out_files[shard].write(url + "\n")
        seen_urls.add(url)
