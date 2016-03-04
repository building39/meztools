#!/usr/bin/env python
'''
Created on Sep 25, 2013

@author: mmartin
'''

import getopt
import getpass
import sys

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
        self.indent = 0
        self.num_files = 0
        self.num_containers = 0
        self.num_objects = 0
        self.num_unknowns = 0
        self.passwd = passwd
        self.process_function = process_function
        self.userid = userid
        self.verbose = verbose
        self.verify = verify

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

    def _process_container(self, container, indent=0):
        '''
        Recursively process containers.
        '''
        self.indent = indent
        self.c2c.set_header('Accept', 'application/vnd.csp.container-info+json')
        self.c2c.set_header('X-Cloud-Depth', '0')

        path = '/v2/containers/%s' % container
        container = self.c2c.GET(path)
        containers = []
        self.c2c.set_header('Accept', 'application/vnd.csp.file-list+json')
        self.c2c.set_header('X-Cloud-Depth', '1')
        objects = self.c2c.GET("%s/contents" % path)
        self.num_objects = objects['file-list']['count']

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
            self._process_container(key, self.indent + 2)

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


def walk_csp(pobj, obj, otype):
    if otype in ['container']:
        print ' ' * pobj.indent + 'Container name: %s' % obj['container']['name']
        print ' ' * pobj.indent + 'Container %s contains %d objects' % \
                                    (obj['container']['name'],
                                     pobj.num_objects)
    elif otype in ['file']:
        print ' ' * pobj.indent + 'File name: %s' % obj['file']['name']
    else:
        print ' ' * pobj.indent + 'Unknown object: %s' % obj

    sys.stdout.flush()

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

    pobj = ProcessObjects(walk_csp,
                          host=host,
                          passwd=passwd,
                          userid=userid,
                          verbose=verbose,
                          verify=verify)

    pobj.process_objects()

if __name__ == '__main__':
    main(sys.argv[1:])
