'''
Created on Sep 23, 2013

@author: mmartin
'''

from base64 import (
    b64encode
)

import json
import requests

from mezeo_cdmi.objectid import ObjectId
from mezeo_dds.access import (
    decodeKey,
)


CDMICONTAINER = 'application/cdmi-container'
CDMIOBJECT = 'application/cdmi-object'
CHILDREN = 'children'
COUNT = 10
OBJECTTYPE = 'objectType'
ROOT = '/cdmi'
STORAGEROOT = '/cdmi/storage_root'
SYSTEMDOMAIN = 'system_domain/'
TOPLEVELDOMAIN = '/cdmi/cdmi_domains'
VERSION = '1.0.0'


class CDMIExports(object):
    '''
    Return a list of all exported containers
    '''

    def __init__(self, host, userid, passwd, verify, debug=False):
        '''
        Initialize this object
        '''
        self.debug = debug
        self.domains = []
        self.exports = []
        self.headers = {'X-CDMI-Specification-Version': '1.0.1'}
        self.storage_root = None
        self.verify = verify
        # prepend url with http if it's not already there
        if (host.startswith('http:') or
            host.startswith('https:')):
            self.host = host
        else:
            self.host = 'http://%s' % host

        self.auth_basic = "Basic %s" % b64encode("%s:%s" %
                        (userid, passwd))

        self.headers['Authorization'] = self.auth_basic

    def get_DDS_key(self, objectid):
        '''
        Convert a CDMI object id into a CSP object id
        '''
        return decodeKey(ObjectId.decode(objectid))

    def _exports(self, path, returnCSP=False):
        domain = self.GET(path, self.headers)
        if CHILDREN not in domain:
            return
        if not domain['objectName'].endswith('/'):
            domain['objectName'] = '%s/' % domain['objectName']
        children = domain[CHILDREN]
        for child in children:
            child_uri = '%s%s' % (path, child)
            data = self.GET(child_uri, self.headers)
            if not data:
                return
            if not data['objectName'].endswith('/'):
                data['objectName'] = '%s/' % data['objectName']
            if 'exports' not in data:
                if CHILDREN in data:
                    for gchild in data[CHILDREN]:
                        self._exports('%s%s' % (child_uri, gchild),
                                      returnCSP=returnCSP)
                continue
            if returnCSP:
                key = self.get_DDS_key(data['objectID'])
                self.exports.append(b64encode(key))
            else:
                self.exports.append(child_uri)

    def get_exports(self, path, returnCSP=False):
        self.exports = []
        self._exports(path, returnCSP=returnCSP)
        return self.exports

    def GET(self, path, headers=None):
        '''
        Get data from CDMI
        '''
        if not headers:
            headers = self.headers
        url = '%s%s' % (self.host, path)
        res = requests.get(url=url,
                           allow_redirects=True,
                           headers=headers,
                           verify=self.verify)
        if res.status_code in [200]:
            return json.loads(res.text)
        else:
            print ('Could not connect to server. Response status %d'
                   % res.status_code)

    def set_header(self, name, value):
        self.headers[name] = value

    def set_headers(self, headers):
        self.headers = headers
        if 'Authorization' not in self.headers:
            self.headers['Authorization'] = self.auth_basic
