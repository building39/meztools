#!/usr/bin/env python
'''
Created on Apr 21, 2013

@author: mmartin
'''

import getopt
import getpass
from pymez.cloud import Cloud
import sys
import urlparse

DEBUG = False
INDENT_STEP = 2
VERSION = '1.0.0'
PRINT_SEP = '-------------------------------------------------------------'


class DDNWebDAVFix(object):

    def __init__(self,
                 userid,
                 password,
                 host,
                 verbose=False):
        # import pydevd; pydevd.settrace('192.168.69.64')
        self.indent = 0
        self.newdirs = 0
        self.newfiles = 0
        self.userid = userid
        self.password = password
        self.verbose = verbose
        self.host = host
        self.urlparse = urlparse.urlparse(self.host)
        # get just the endpoint
        self.scheme = '%s' % self.urlparse.scheme
        self.netloc = '%s' % self.urlparse.netloc
        if not self.netloc:
            self.netloc = self.host
        try:
            self.start = '%s' % self.urlparse.path.split('/')[1]
        except:
            self.start = ''

        # Get cloud
        self.cloud = Cloud(self.userid,
                           self.password,
                           self.host)

        if DEBUG:
            print 'Source: %s' % self.source
            print 'Target: %s' % self.target

    def _process_container(self, container):
        self.newdirs += 1
        if self.verbose:
            print 'Now fixing %s' % container.get_name()
        container.get_contents()
        # rename the container here.
        self._rename_container(container)

        # rename file objects here
        objects = container.get_objects()
        for x in range(len(objects)):
            self._rename_file(objects[x])

        subcontainers = container.get_subcontainers()
        for x in range(len(subcontainers)):
            self._process_container(subcontainers[x])

    def _rename_container(self, container):
        '''
        This will be a PUT request with a form
        # that looks like this:
        # {"container": {"name": "new_name"}}
        '''
        if self.verbose:
            print 'Now fixing container %s' % container.get_name()

    def _rename_file(self, fileobj):
        '''
        This will be a PUT request with a form
        # that looks like this:
        # {"file": {"name": "new_file_name.jpg"}}
        '''
        if self.verbose:
            print 'Now fixing file %s' % fileobj.get_name()

    def fixit(self):
        num_locs = self.cloud.how_many_locations()
        for loc in range(num_locs):
            container = self.cloud.get_location_root_container(loc)
            self._process_container(container)


def usage():
    print ('Mezeo Bulk Download')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --userid=[userid] --passwd=[password]'
           % sys.argv[0])
    print ('          [--norecurse] [--noverify] [--source=[src-path]')
    print ('')
    print (' Command Line options:')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - MCSP host url. Required.')
    print ('                The simplest form is:')
    print ('                    https://hostname/v2')
    print ('                To specify that downloaded files come from some')
    print ('                particular container, --host can be of the form')
    print ('                    https://hostname/v2/some/particular/container')
    print ('  --userid    - MCSP userid. Required.')
    print ('  --passwd    - MCSP userid password. '
           'If not supplied, will be prompted for.')
    print ('  --norecurse - Do not recurse into sub-containers. Optional,')
    print ('                default action is to recurse.')
    print ('  --source    - Source container. Objects relative to this '
           'container will be downloaded.')
    print ('                Default is the root container.')
    print ('  --target    - Target path. Objects will be downloaded into this '
           'directory.')
    print ('                Default is the current working directory.')
    print ('  --verbose   - Print progress information.')
    print ('')

    sys.exit(0)


def main(argv):

    if (len(sys.argv) < 3):
        usage()

    host = None
    userid = None
    passwd = None
    verbose = False
    # import pydevd;pydevd.settrace()
    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'debug',
                                    'host=',
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
        elif opt == '--debug':
            global DEBUG
            DEBUG = True
        elif opt == '--host':
            host = arg
        elif opt == '--userid':
            userid = arg
        elif opt == '--passwd':
            passwd = arg
        elif opt == '--verbose':
                verbose = True

    if host is None or userid is None:
        usage()
        sys.exit(1)

    if passwd is None:
        passwd = getpass.getpass()

    bu = DDNWebDAVFix(userid,  # User's userid
                      passwd,  # User's password
                      host,  # mcsp host url
                      verbose=verbose)  # print verbose information on progress

    bu.fixit()

    print 'Fixed %d directories.' % bu.newdirs
    print 'Fixed %d files' % bu.newfiles

if __name__ == "__main__":
    main(sys.argv[1:])
