#!/usr/bin/env python
'''
Created on Sep 25, 2013

@author: mmartin
'''

from base64 import (
    b64encode
)
import getopt
import getpass
import json
import requests
import sys

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from CDMIExports import CDMIExports

STORAGEROOT = '/cdmi/storage_root'
VERSION = '1.0.0'

verbose = False

class ProcessObjects(object):
    '''
    Process all container and file objects.
    Set "self.process_function" to point to a function that will perform
    some action on CSP file and/or container objects
    '''
    def __init__(self,
                 process_function,
                 host='localhost',
                 passwd=None,
                 userid='administrator',
                 verbose=False,
                 verify=True):
        self.host = host
        self.num_files = 0
        self.num_containers = 0
        self.num_unknowns = 0
        self.passwd = passwd
        self.process_function = process_function
        self.userid = userid
        self.verbose = verbose
        self.verify = verify

        self.auth_basic = "Basic %s" % b64encode("%s:%s" % (userid, passwd))

        self.c2c = CDMIExports(host, userid, passwd, verify)
        self.storage_root = self.c2c.GET(STORAGEROOT)
        self.path = '%s%s/' % (self.storage_root['parentURI'],
                               self.storage_root['objectName'])

        self.exports = self.c2c.get_exports(self.path,
                                            returnCSP=True)

        if (self.host.startswith('http:') or
            self.host.startswith('https:')):
            pass
        else:
            self.host = 'http://%s' % self.host

        if self.verbose:
            print 'Processing %d exported containers.' % len(self.exports)

    def _process_container(self, container):
        '''
        Recursively process containers.
        '''
        self.c2c.set_header('Accept', 'application/vnd.csp.container-info+json')
        self.c2c.set_header('X-Cloud-Depth', '0')
        self.headers = self.c2c.headers

        path = '/v2/containers/%s' % container
        container = self.c2c.GET(path)
        containers = []
        self.c2c.set_header('Accept', 'application/vnd.csp.file-list+json')
        self.c2c.set_header('X-Cloud-Depth', '1')
        objects = self.c2c.GET("%s/contents" % path)

        for obj in objects['file-list']['file-list']:
            if 'file' in obj:
                self.num_files += 1
                self.process_function(self, obj, 'file')
            elif 'container' in obj:
                containers.append(obj)
            else:
                self.num_unknowns += 1
                self.process_function(self, obj, 'unknown')
                continue
        for obj in containers:
            self.process_function(self, obj, 'container')
            self.num_containers += 1
            key = obj['container']['uri'].split('/')[-1]
            self._process_container(key)

    def process_object(self, obj, otype):
        if self.verbose:
            print 'Touching %s %s' % (otype, obj[otype]['name'])
            sys.stdout.flush()

    def process_objects(self):
        '''
        Main driver.
        '''
        if not self.process_function:
            print 'Fatal Error: no processing function specified.'
            sys.exit(1)
        self.headers = {'X-Client-Specification': '3'}
        self.c2c.set_headers(self.headers)
        for export in self.exports:
            self._process_container(export)

def http_get(url, pobj):
    '''
    Get CSP object
    '''
    print 'headers: %s' % pobj.headers
    res = requests.get(url=url,
                       allow_redirects=True,
                       headers=pobj.headers,
                       verify=pobj.verify)
    if res.status_code in [200]:
        data = json.loads(res.text)
    else:
        data = None
    return res.status_code, data

def http_put(url, pobj, payload):
    '''
    PUT a CSP object.
    In this particular case, we are renaming the object with the same name
    in order to force a metadata update. The form for renaming an object
    looks like:
    { "file": {"name": "<file's current name>"}}
    or:
    { "container": {"name": "<container's current name>"}}
    '''
    res = requests.put(url=url,
                       data=payload,
                       allow_redirects=True,
                       headers=pobj.headers,
                       verify=pobj.verify)
    if res.status_code not in [204]:
        print 'status %d data %s' % (res.status_code, payload)
        print 'payload is a %s' % type(payload)
    return res.status_code

def process_object(pobj, obj, otype):
    if verbose:
        print 'Touching %s %s' % (otype, obj[otype]['name'])
        sys.stdout.flush()
    pobj.headers['Content-Type'] = 'application/vnd.csp.%s-info+json' % otype
    pobj.headers['Accept'] = pobj.headers['Content-Type']
    url = obj[otype]['uri']
    if otype in ['file']:
        data = json.dumps({otype: {"name": str(obj[otype]['name']),
                               "mime_type": str(obj[otype]['mime_type'])}})
    elif otype in ['container']:
        data = json.dumps({otype: {"name": str(obj[otype]['name'])}})
    status = http_put(url, pobj, data)
    if status in [204]:
        if verbose:
            print 'Success! %s old mtime: %s by: %s' % (obj[otype]['name'],
                                                        obj[otype]['modified'],
                                                        obj[otype]['modified_by'])
    elif status in [403]:
        print 'Not Authorized! %s' % obj[otype]['name']
    elif status in [404]:
        print '%s not found!' % url
    else:
        print 'Fatal error %d updating %s' % (status, url)
        print 'headers: %s' % pobj.headers
        sys.exit(1)


def usage():
    '''
    Usage instructions.
    '''
    print ('Touch all CSP file/container objects')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --userid=[userid] --passwd=[password]'
           % sys.argv[0])
    print ('')
    print (' Command Line options:')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - MCSP host url. Defaults to "localhost".')
    print ('  --userid    - MCSP userid. Defaults to "administrator".')
    print ('  --passwd    - MCSP userid password. If omitted, will be'
           '                prompted for.')
    print ('  --verbose   - Print progress information.')
    print ('  --noverify  - Do not verify site certificate when connecting.')
    print ('')

    sys.exit(0)

def main(argv):
    '''
    Main entry point
    '''
    if (len(argv) < 2):
        usage()
    host = 'localhost'
    userid = 'administrator'
    passwd = None
    global verbose
    verbose = False
    verify = True

    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'host=',
                                    'noverify',
                                    'userid=',
                                    'passwd=',
                                    'verbose'])
    except getopt.GetoptError, e:
        print ('opt error %s' % e)
        print ('')
        usage()

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
        elif opt == '--host':
            host = arg
        elif opt == '--userid':
            userid = arg
        elif opt == '--passwd':
            passwd = arg
        elif opt == '--noverify':
            verify = False
        elif opt == '--verbose':
            verbose = True

    if host is None or userid is None:
        usage()
        sys.exit(1)

    if passwd is None:
        passwd = getpass.getpass()

    pobj = ProcessObjects(process_object,
                          host=host,
                          passwd=passwd,
                          userid=userid,
                          verbose=verbose,
                          verify=verify)

    pobj.process_objects()

if __name__ == '__main__':
    main(sys.argv[1:])
