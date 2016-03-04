#!/usr/bin/env python
'''
Created on May 12, 2013

@author: mmartin
'''

from __future__ import print_function
from hurry.filesize import size
from datetime import timedelta

import getopt
import getpass
import os
from time import sleep
from pymez.cloud import Cloud
from pymez.container import Container
from pymez.filecontent import FileContent
from pymez.fileobject import FileObject

import sys
import time
import urlparse

INDENT_STEP = 2
VERSION = '1.0.0'
PRINT_SEP = '-------------------------------------------------------------'

global containers_created


class BulkUpload(object):

    def __init__(self,
                 userid,
                 password,
                 host,
                 source,
                 target,
                 throttle=0,
                 hidden=False,
                 recurse=True,
                 verbose=False):
        self.containers_created = 0
        self.indent = 0
        self.newdirs = 0
        self.newfiles = 0
        self.userid = userid
        self.password = password
        self.recurse = recurse

        self.source = source
        if target[0] == '/':
            target = target[1:]  # must make this path relative
        self.target = target
        if os.path.isdir(self.source):
            if self.source[-1] == '/':
                self.source = self.source[:-1]  # remove trailing slash
            else:
                self.target = '%s/%s' % (self.target, os.path.split(self.source)[1])
        else:
            print('Source %s:' % self.source)
            print('Must be a directory, not a file.')
            sys.exit(1)
        self.throttle = throttle
        self.verbose = verbose
        self.hidden = hidden
        self.host = host
        self.urlparse = urlparse.urlparse(self.host)
        # get just the endpoint
        self.scheme = '%s' % self.urlparse.scheme
        self.secure = True if self.scheme == 'https' else False
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

    def _get_dir_contents(self, dirpath):
        try:
            os.chdir(self.source)
        except:
            print('could not change to directory %s' % self.source)
            raise()
        return os.listdir(dirpath)

    def _upload_file(self, fileinfo):
        filename = fileinfo.get_name()
        print("Uploading file %s" % filename)
        self.newfiles += 1
        fo = open(filename, 'wb')
        stream = fileinfo.get_content()
        buf = stream.read(1024)
        size = len(stream.data)
        if not buf:
            fo.write(stream.data)
            print('wrote %d bytes' % size)
        else:
            while buf:
                fo.write(buf)
                buf = stream.read()
        fo.close()
        sys.stdout.flush()
        if self.throttle > 0:
            sleep(self.throttle)

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

    def _upload(self, path, dirs, files, newcontainers):
        if not self.hidden:
            files[:] = [f for f in files if not f.startswith('.')]
            dirs[:] = [d for d in dirs if not d.startswith('.')]

        container = None

        try:
            for c, p in newcontainers:
                if p == path:
                    container = c
                    break
            newcontainers.remove((c, p))
        except:
            print('Ouch!')
            raise

        contents_uri = container.get_contents_uri()

        children, _fileobjs, _total = container.get_contents()
        subdirs = []
        for child in children:
            subdirs.append(child.get_name())

        for directory in dirs:
            if directory not in subdirs:
                print('Creating container %s/%s' % (path, directory))
                newcontainer = Container(self.cloud,
                                         name=directory,
                                         parent=contents_uri)
                newcontainers.append((newcontainer,
                                      '%s/%s' % (path, directory)))
                self.containers_created += 1
        for fname in files:
            self.process_file(path, fname, container)

    def upload(self):
        # import pydevd; pydevd.settrace()
        if not self.target:
            locations = self.cloud.get_locations()
            for location in locations:
                if location.isDefault():
                    self.target = location
                    break
            if not self.target:
                print('No default container')
                sys.exit()
        else:
            self.target = self._find_target()
        newcontainers = [(self.target, self.source)]

        if self.recurse:
            for (path, dirs, files) in os.walk(self.source):
                self._upload(path, dirs, files, newcontainers)
        else:
            path = self.source
            dirs = []
            files = []
            for item in os.listdir(path):
                if not self.hidden and item.startswith('.'):
                    continue
                obj = os.path.join(path, item)
                if os.path.isfile(obj):
                    files.append(item)
                elif os.path.isdir(obj):
                    dirs.append(item)
            self._upload(path, dirs, files, newcontainers)

    def process_file(self, path, fname, parent):
        '''
        Upload a file.
        fname is the file's name, and parent is the ContainerObject
        of the parent container.
        '''
        if self.verbose:
            print('\nUploading:')
            print('    File: %s' % (fname))
            print('    Parent: %s' % (parent.get_name()))
        # parent = parent
        fileobj = FileObject(parent.get_cloud(),
                             name=fname,
                             parent=parent)
        fpath = os.path.join(path, fname)
        try:
            f = open(fpath, 'r')
        except:
            print('Could not open file %s' % fpath)
        file_content = FileContent(fileobj)
        if file_content.POST(fpath):
            self.newfiles += 1
        f.close()
        return True

    def _find_target(self):

        # Split up the container hierarchy
        containers = self.target.split('/')
        # Find the root container
        location = self.cloud.get_location_by_name(containers[0])
        if not location:
            print('Root container %s does not exist' % containers[0])
            sys.exit()
        else:
            containers.remove(containers[0])

        # now find the target container.
        # first, get the contents of the root
        container = location.get_root_container()
        
        depth = len(containers)
        if depth == 1 and containers[0] == container.get_name():
            return container
        
        url = location.get_root_container_uri()

        return self._get_or_create(containers, container, url)

    def _get_or_create(self, containers, container, url):

        found = False
        for ndx in range(len(containers)):
            children, _, total = container.get_contents()
            if total < 1:  # container is empty
                container = Container(self.cloud,
                                  name=containers[ndx],
                                  parent=container.get_uri())
                url = container.get_uri()
            else:
                found = False
                for child in children:
                    if containers[ndx] == child.get_name():
                        container = child
                        found = True
                        break
        if not found:
            container = Container(self.cloud,
                                  name=containers[ndx],
                                  parent=url)
            url = container.get_uri()
            self.containers_created += 1
        return container


def usage():
    print('Mezeo Bulk Upload')
    print('Version : %s' % VERSION)
    print('')
    print('Usage: '
           '%s --host=[hostname] --userid=[userid] --passwd=[password]'
           % sys.argv[0])
    print('          [--norecurse] [--noverify] [--source=[src-path]')
    print('')
    print(' Command Line options:')
    print('  --help      - Print this enlightening message')
    print('  --host      - MCSP host url. Required.')
    print('                The simplest form is:')
    print('                    https://hostname/v2')
    print('                To specify that uploaded files go to some')
    print('                particular container, --host can be of the form')
    print('                    https://hostname/v2/some/particular/container')
    print('  --userid    - MCSP userid. Required.')
    print('  --passwd    - MCSP userid password. '
           'If not specified, will be prompted for.')
    print('  --norecurse - Do not recurse into sub-containers. Optional,')
    print('  --hidden -    Include hidden files and directories. Optional,'
            ' If not specified, defaults to excluding hidden objects.')
    print('                default action is to recurse.')
    print('  --source    - Source container. Objects relative to this '
           'path will be uploaded.')
    print('                Default is the current working directory.')
    print('  --target    - Target path. Objects will be uploaded into this '
           'container.')
    print('  --throttle  - Upload throttle. Pause n seconds between requests')
    print('                Default is the default root container.')
    print('  --verbose   - Print progress information.')
    print('')

    sys.exit(0)


def main(argv):

    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

    if (len(sys.argv) < 3):
        usage()

    hidden = False
    host = None
    userid = None
    passwd = None
    recurse = True
    source = ''
    target = None
    verbose = False
    throttle = 0

    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'debug',
                                    'host=',
                                    'userid=',
                                    'passwd=',
                                    'hidden',
                                    'norecurse',
                                    'source=',
                                    'target=',
                                    'throttle=',
                                    'verbose'])
    except getopt.GetoptError, e:
        print('opt error %s' % e)
        print('')
        usage()

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
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
            target = arg
        elif opt == '--throttle':
            throttle = float(arg)
        elif opt == '--verbose':
                verbose = True

    if host is None or userid is None:
        usage()
        sys.exit(1)

    if passwd is None:
        passwd = getpass.getpass()

    start = time.time()
    bu = BulkUpload(userid,  # User's userid
                    passwd,  # User's password
                    host,  # mcsp host url
                    source,  # path to objects to be uploaded
                    target,  # path to upload to
                    throttle,  # pause <throttle> seconds between requests
                    hidden=hidden,  # replicate hidden objects if True
                    recurse=recurse,  # recurse directories if True
                    verbose=verbose)  # print verbose information on progress

    bu.upload()
    end = time.time()

    print('Created %d directories.' % bu.containers_created)
    print('Uploaded %d new files' % bu.newfiles)
    print('Elapsed time: %s\n' % timedelta(seconds=(end - start)).__str__())

if __name__ == "__main__":
    main(sys.argv[1:])
