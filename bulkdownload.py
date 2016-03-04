#!/usr/bin/env python
'''
Created on Apr 21, 2013

@author: mmartin
'''

from __future__ import print_function
from hurry.filesize import size
from datetime import timedelta

import errno
import getopt
import getpass
import os
from pymez.cloud import Cloud
import sys
import time
import urlparse

BLKSIZE = 8 * 1024 * 1024
DEBUG = False
INDENT_STEP = 2
VERSION = '1.0.0'
PRINT_SEP = '-------------------------------------------------------------'

sys.path.append('/opt/eclipse/plugins/org.python.pydev_3.6.0.201406232321/pysrc')
import pydevd

class BulkDownload(object):

    def __init__(self,
                 userid,
                 password,
                 host,
                 source,
                 target,
                 newer = False,
                 hidden=False,
                 recurse=True,
                 verbose=False):
        # import pydevd; pydevd.settrace('192.168.69.64')
        self.hidden = hidden
        self.indent = 0
        self.newdirs = 0
        self.newfiles = 0
        self.newer = newer
        self.userid = userid
        self.password = password
        self.recurse = recurse
        self.source = source
        if target:
            self.target = target
        else:
            self.target = os.getcwd()
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
                           self.host,
                           passwd=self.password)

        if self.verbose:
            print(PRINT_SEP)
            print('base url is %s' % self.netloc)
            self._print_cloud()
            print(PRINT_SEP)

        if DEBUG:
            print('Source: %s' % self.source)
            print('Target: %s' % self.target)

    def _download_file(self, fileinfo):
        filename = fileinfo.get_name()
        if not self.hidden and filename.startswith('.'):
            if DEBUG:
                print('Skipping hidden file %s' % filename)
            return
        try:
            print('Downloading file %s' % str(filename))
        except UnicodeEncodeError:
            print('Whoa! A unicode encode error!')
            return
        self.newfiles += 1
        # pydevd.settrace()
        if self.newer:
            if os.path.isfile(filename):
                fsize = os.stat(filename).st_size
                if fsize == float(fileinfo.get_bytes()) and \
                   float(fileinfo.get_modified()) >= float(fileinfo.get_modified()):
                    if True:
                        print('Skipping unmodified file %s' % filename)
                    return
        fo = open(filename, 'wb')
        _status, stream = fileinfo.get_content()
        fsize = fileinfo.get_bytes()
        buf = stream.read(BLKSIZE)
        if not buf:
            fo.write(buf)
        else:
            wrote = 0
            while buf:
                fo.write(buf)
                #time.sleep(1)  # This seems to help prevent IncompleteRead exceptions
                if self.verbose:
                    wrote += len(buf)
                    if float(fsize) != 0:
                        pct = (wrote / float(fsize)) * 100
                        print('Size: %s %f%% done - Wrote %s bytes    ' % (size(fsize),
                                                                       pct,
                                                                       size(wrote)),
                           end='\r')
                try:
                    buf = stream.read(BLKSIZE)
                except:
                    print('')
                    raise
            if self.verbose:
                print('')
        fo.close()
        sys.stdout.flush()

    def _print_cloud(self):
        print('Cloud info:')
        print('    Account URI:        %s' % self.cloud.get_account_uri())
        print('    Allspaces URI:      %s' % self.cloud.get_allspaces_uri())
        print('    Locations:')
        for location in range(self.cloud.how_many_locations()):
            loc = self.cloud.get_location(location)
            print('    Location #%d:' % (location + 1))
            print('        Default:        %s' %
                  loc.isDefault())
            print('        Mgmt URI:       %s' %
                   loc.get_mgmt_uri())
            print('        Name:           %s' %
                   loc.get_name())
            print('        Namespace:      %s' %
                   loc.get_namespace())
            print('        Notifications:  %s' %
                   loc.get_notifications())
            print('        Root Container: %s' %
                   loc.get_root_container_uri())
            print('        Spaces:         %s' %
                   loc.get_spaces())
        print('    Namespaces URI:     %s' % self.cloud.get_namespaces_uri())
        print('    Recycle bin URI:    %s' % self.cloud.get_recyclebin_uri())
        print('    Search URI:         %s' % self.cloud.get_search_uri())
        print('    Shares URI:         %s' % self.cloud.get_shares_uri())

    def _print_container(self, container):
        print('Container %s' % container)
        print('        Name:          %s' % container.get_name())
        print('        Date Created:  %s' %
               time.ctime(container.get_created()))
        print('        Last Accessed: %s' %
               time.ctime(container.get_accessed()))
        print('        Last Modified: %s' %
               time.ctime(container.get_modified()))
        print('        Modified By:   %s' % container.get_modified_by())
        print('        Bytes:         %d' % container.get_bytes())
        print('        Shared:        %s' % container.get_shared())
        print('        Owner:         %s' % container.get_owner())
        print('        Version:       %s' % container.get_version())
        print('        Comments:      %s' % container.get_comments())
        print('        Contents:      %s' % container.get_contents_uri())
        print('        Metadata:      %s' % container.get_metadata())
        print('        Permissions:   %s' % container.get_permissions())
        print('            Principal: %s' % container.get_principal())
        print('            Simple:    %s' % container.get_simple())
        print('        Uri:           %s' % container.get_uri())
        print('        Parent:        %s' % container.get_parent())

    def _process_container(self, container):
        if not self.hidden and container.get_name().startswith('.'):
            if DEBUG:
                print('Skipping hidden container %s' % container.get_name())
            return
        path = '%s/%s' % (os.getcwd(), container.get_name())
        _create_directory(path)
        os.chdir(path)
        self.newdirs += 1
        if self.verbose:
            print('Now downloading to %s' % path)
        container.get_contents()
        objects = container.get_objects()
        for x in range(len(objects)):
            self._download_file(objects[x])

        subcontainers = container.get_subcontainers()
        for x in range(len(subcontainers)):
            self._process_container(subcontainers[x])

        os.chdir('..')

    def download(self):
        num_locs = self.cloud.how_many_locations()
        if self.verbose:
            print('Target directory: %s' % self.target)
            print('Origin container is: %s' % \
            self.source if self.source \
                else 'Downloading everything from %d locations' % num_locs)
        os.chdir(self.target)
        for loc in range(num_locs):
            container = self.cloud.get_location_root_container(loc)
            if self.source:
                if self.source == container.get_name():
                    self._process_container(container)
                    break
            else:
                self._process_container(container)


def _create_directory(path):
    try:
        os.makedirs(path)
        print('Directory %s created' % path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            print('Could not create directory %s' % path)
            raise


def usage():
    print('Mezeo Bulk Download')
    print('Version : %s' % VERSION)
    print('')
    print('Usage: '
           '%s --host=[hostname] --userid=[userid] --passwd=[password]'
           % sys.argv[0])
    print('          [--norecurse] [--noverify] [--source=[src-path]')
    print('')
    print(' Command Line options:')
    print('  --help      - Print this enlightening message')
    print('  --newer     - Download all. Default is to only download newer files.')
    print('  --host      - MCSP host url. Required.')
    print('                The simplest form is:')
    print('                    https://hostname')
    print('                To specify that downloaded files come from some')
    print('                particular container, --host can be of the form')
    print('                    https://hostname/v2/some/particular/container')
    print('  --userid    - MCSP userid. Required.')
    print('  --passwd    - MCSP userid password. '
           'If not supplied, will be prompted for.')
    print('  --norecurse - Do not recurse into sub-containers. Optional,')
    print('                default action is to recurse.')
    print('  --source    - Source container. Objects relative to this '
           'container will be downloaded.')
    print('                Default is the root container.')
    print('  --target    - Target path. Objects will be downloaded into this '
           'directory.')
    print('                Default is the current working directory.')
    print('  --verbose   - Print progress information.')
    print('')

    sys.exit(0)


def main(argv):
    
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    if (len(sys.argv) < 3):
        usage()

    newer = False
    hidden = False
    host = None
    userid = None
    passwd = None
    recurse = True
    source = ''
    target = os.getcwd()
    verbose = False
    # import pydevd;pydevd.settrace()
    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'newer',
                                    'debug',
                                    'hidden',
                                    'host=',
                                    'userid=',
                                    'passwd=',
                                    'norecurse',
                                    'source=',
                                    'target=',
                                    'verbose'])
    except getopt.GetoptError, e:
        print('opt error %s' % e)
        print('')
        usage()

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
        elif opt == '--newer':
            newer = True
        elif opt == '--debug':
            global DEBUG
            DEBUG = True
        elif opt == '--hidden':
            hidden = True
        elif opt == '--host':
            host = arg
        elif opt == '--userid':
            userid = arg
        elif opt == '--passwd':
            passwd = arg
        elif opt == '--norecurse':
                recurse = False
        elif opt == '--source':
            source = arg
        elif opt == '--target':
            if not os.path.exists(arg):
                _create_directory(arg)
            os.chdir(arg)
            target = os.getcwd()
        elif opt == '--verbose':
                verbose = True

    if host is None or userid is None:
        usage()
        sys.exit(1)

    if passwd is None:
        passwd = getpass.getpass()
        
    start = time.time()
        
    bu = BulkDownload(userid,  # User's userid
                    passwd,  # User's password
                    host,  # mcsp host url
                    source,  # path to objects to be downloaded
                    target,  # path to download to
                    newer=newer, # all files or only newer?
                    hidden=hidden,  # download hidden objects
                    recurse=recurse,  # recurse directories if True
                    verbose=verbose)  # print verbose information on progress

    bu.download()
    
    end = time.time()

    print('Created %d directories.' % bu.newdirs)
    print('Downloaded %d new files' % bu.newfiles)
    
    print('Elapsed time: %s\n' % timedelta(seconds=(end - start)).__str__())

if __name__ == "__main__":
    main(sys.argv[1:])
