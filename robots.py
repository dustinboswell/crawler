"""A class to parse/manage robots.txt files.
It persists the data to disk too.
"""

import shelve
import urllib2
from urlparse import urlparse
from robotexclusionrulesparser import RobotExclusionRulesParser

import urls

class RobotsManager:
  def __init__(self, useragent, db_path="/tmp/robots.db"):
    self.robot_shelf = shelve.open(db_path, writeback=True)
    self.useragent = useragent

  def needs_robot_fetch(self, url):
    host = urls.getHost(url)
    robot = self.robot_shelf.get(host, None)
    if not robot: return True
    if robot.is_expired(): return True
    return False

  def robots_url(self, url):
    host = urls.getHost(url)
    return "http://%s/robots.txt" % host

  def fetch_robots(self, url):
    host = urls.getHost(url)
    robot = self.robot_shelf.get(host, None)
    if robot is None:
      robot = RobotExclusionRulesParser()

    print "Fetching robots.txt for", host
    try:
      robot.fetch(self.robots_url(url))
    except urllib2.URLError, e:
      print "Failed to fetch robots.txt for", host, e

    self.robot_shelf[host] = robot
    self.robot_shelf.sync()

  def is_allowed(self, url):
    """Based on the information we already have, can we crawl this url?
    If robots hasn't been fetched, will return True.
    If robots data has been fetched (even if it's expired), it will be used.
    """
    host = urls.getHost(url)
    robot = self.robot_shelf.get(host, None)
    if not robot: return True
    return robot.is_allowed(self.useragent, url)

  def update(self, host, response, response_time=None):
    """Takes a string containing http headers (optional) + text of the robots file.
    If response is None, this means the fetch failed. ("" means an empty robots.txt file)
    The headers are used to look for:
     - Whether it was a 200OK or 404, etc..
     - Look for an Expires: header
    Changes are saved to the database file.
    response_time is a string in seconds from epoch (defaults to now)
    """
    robot = self.robot_shelf.get(host, None)
    if robot is None:
      robot = RobotExclusionRulesParser()

    robot.parse(response)
    self.robot_shelf[host] = robot
    self.robot_shelf.sync()

  def robots_status(self, host):
    """Returns one of:
    UNKNOWN - first time we've ever heard of this host
    UP_TO_DATE - robots has been fetched and hasn't expired
    OUT_OF_DATE - robots has been fetched, but is now expired
    FAILED - last attempt did not succeed (failed to connect, 404, timeout, etc...)
    """
    pass
