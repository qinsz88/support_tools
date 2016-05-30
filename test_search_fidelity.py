'''
@author: Howard Chin
@contact: hchin@splunk.com
@since: 05/30/2016
'''


import requests
import logging
import json
import time
from optparse import OptionParser
from xml.dom.minidom import parse, parseString

logging.basicConfig(format='%(asctime)s %(message)s', filename='test_search_fidelity.log', level=logging.INFO)
LOGGER = logging.getLogger('TestSearchFidelity')

parser = OptionParser(usage="Example 1: %prog --username=admin --password=changed --splunk_host=server1:8089 --trigger_type=adhoc_search --search_string=\"search index=idx1 | stats count\"\n       "
                            "Example 2: %prog --username=admin --password=changed --splunk_host=server1:8089 --trigger_type=savedsearch_dispatch --ss_name=ss1")
parser.add_option('--username', dest="username", default='admin', help="username")
parser.add_option('--password', dest="password", default="changeme", help="password")
parser.add_option('--splunk_host', dest="splunk_host", default="localhost:8089", help="splunk hostname:port")
parser.add_option('--trigger_type', dest="trigger_type", default="savedsearch_dispatch", help="savedsearch_dispatch or adhoc_search")
parser.add_option('--ss_name', dest="ss_name", help="saved search name")
parser.add_option('--search_string', dest="search_string", help="search string for adhoc_search type")
(options, args) = parser.parse_args()

class TestSearchFidelity(object):

    def __init__(self):
        super(TestSearchFidelity, self).__init__()
        self.username = options.username
        self.password = options.password
        self.auth = (self.username, self.password)
        self.splunk_host = options.splunk_host
        self.trigger_type = options.trigger_type
        self.ss_name = options.ss_name
        self.search_string = options.search_string
        self.my_search = SearchHelpers()

    def run(self):
        if self.trigger_type == "savedsearch_dispatch":
            fast_event_count = self.savedsearch_dispatch(self.splunk_host, self.ss_name, "fast")
            smart_event_count = self.savedsearch_dispatch(self.splunk_host, self.ss_name, "smart")
            verbose_event_count = self.savedsearch_dispatch(self.splunk_host, self.ss_name, "verbose")
            assert fast_event_count == smart_event_count == verbose_event_count
            print "Test status: PASS!"
        elif self.trigger_type == "adhoc_search":
            fast_event_count = self.adhoc_search(self.splunk_host, self.search_string, "fast")
            smart_event_count = self.adhoc_search(self.splunk_host, self.search_string, "smart")
            verbose_event_count = self.adhoc_search(self.splunk_host, self.search_string, "verbose")
            assert fast_event_count == smart_event_count == verbose_event_count
            print "Test status: PASS!"

    def adhoc_search(self, splunk_host, search_string, adhoc_search_level):
        url = 'https://{0}/servicesNS/admin/search/search/jobs'.format(splunk_host)
        #-d search="search index=_internal| stats count" -d dispatch.adhoc_search_level=<verbose/fast/smart>
        data = { 'search' : search_string,
                 'dispatch.adhoc_search_level' : adhoc_search_level }
        headers = {'content-type' : 'text/xml; charset=utf-8'}
        r = requests.post(url, data = data, headers = headers, auth = self.auth, verify = False)
        if r.status_code == 201:
            xml_string = r.text.rstrip()
            dom = parseString(xml_string)
            # retrieve the first xml tag (<tag>data</tag>) that the parser finds with name "sid"
            xmlTag = dom.getElementsByTagName('sid')[0].toxml()
            # strip off the tag (<tag>sid</tag>  --->   sid):
            ss_sid = xmlTag.replace('<sid>','').replace('</sid>','')
            print ss_sid
            LOGGER.info("ss sid: {0}".format(ss_sid))
            assert self.my_search.isSearchDone(splunk_host, self.auth, ss_sid) == True
            event_count = self.my_search.getEventCount(splunk_host, self.auth, ss_sid)
            return event_count
        else:
            raise Exception("https error! return {0}".format(r.status_code))

    def savedsearch_dispatch(self, splunk_host, ss_name, adhoc_search_level):
        url = 'https://{0}/servicesNS/admin/search/saved/searches/{1}/dispatch'.format(splunk_host, ss_name)
        #-d trigger_actions=1 -d dispatch.adhoc_search_level=<verbose/fast/smart>
        values = { 'trigger_actions' : '1',
                   'dispatch.adhoc_search_level' : adhoc_search_level,
                   'output_mode' : 'json' }
        r = requests.post(url, data = values, auth = self.auth, verify = False)
        if r.status_code == 201:
            json_string = r.text.rstrip()
            print json_string
            data = json.loads(json_string)
            ss_sid = data['sid']
            print ss_sid
            LOGGER.info("ss sid: {0}".format(ss_sid))
            assert self.my_search.isSearchDone(splunk_host, self.auth, ss_sid) == True
            event_count = self.my_search.getEventCount(splunk_host, self.auth, ss_sid)
            return event_count
        else:
            raise Exception("https error! return {0}".format(r.status_code))


###########################
###### Search Helpers #####
###########################
class SearchHelpers(object):
    def isSearchDone(self, splunk_host, auth, sid, timeout_count=120):
        count = 0
        while True:
            url = 'https://{0}/services/search/jobs/{1}'.format(splunk_host, sid)
            values = {'output_mode' : 'json'}
            r = requests.post(url, data = values, auth = auth, verify = False)
            json_string = r.text.rstrip()
            print json_string
            data = json.loads(json_string)
            if data['entry'][0]['content']['dispatchState'] == 'DONE' and data['entry'][0]['content']['doneProgress'] == 1:
                return True
            time.sleep(5)
            count = count + 1
            if count > timeout_count:
                raise Exception("cannot get event count")

    def getEventCount(self, splunk_host, auth, ss_sid):
        url = 'https://{0}/services/search/jobs/{1}'.format(splunk_host, ss_sid)
        values = {'output_mode' : 'json'}
        r = requests.post(url, data = values, auth = auth, verify = False)
        json_string = r.text.rstrip()
        #print json_string
        data = json.loads(json_string)
        event_count = int(data['entry'][0]['content']['eventCount'])
        print event_count
        return event_count


if __name__ == '__main__':
    my_test = TestSearchFidelity()
    my_test.run()




