"""Functions for parsing urls."""

from urlparse import urlparse

# load tlds, ignore comments and empty lines:
with open("effective_tld_names.txt") as tldFile:
    tlds = set([line.strip() for line in tldFile if line[0] not in "/\n"])


def getHost(url):
    return urlparse(url)[1].split(':')[0]


def getDomain(url):
    host = getHost(url)
    urlElements = host.split('.')
    # urlElements = ["abcde","co","uk"]

    for i in range(-len(urlElements), 0):
        lastIElements = urlElements[i:]
        #    i=-3: ["abcde","co","uk"]
        #    i=-2: ["co","uk"]
        #    i=-1: ["uk"] etc

            candidate = ".".join(lastIElements)  # abcde.co.uk, co.uk, uk
            wildcardCandidate = ".".join(
                ["*"] + lastIElements[1:])  # *.co.uk, *.uk, *
            exceptionCandidate = "!" + candidate

            # match tlds:
            if (exceptionCandidate in tlds):
                return ".".join(urlElements[i:])
            if (candidate in tlds or wildcardCandidate in tlds):
                return ".".join(urlElements[i - 1:])
            # returns "abcde.co.uk"

    return host
    #raise ValueError("Domain not in global list of TLDs: " + url)

if __name__ == "__main__":
    print getDomain("http://abcde.co.uk")
    print getDomain("http://123.12.123.32")
