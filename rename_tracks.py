#!/usr/bin/env python
'''
Created 26 January 2014
@author: mmartin
'''

import getopt
import getpass
import string
import sys

from pymez.accept_header import FILE_LIST
from pymez.cloud import Cloud
from pymez.container import Container
from pymez.fileobject import FileObject
from pymez.location import Location

VERSION = '1.0.0'

verbose = False

class ProcessObjects(object):
    '''
    Process all container and file objects.
    Set "self.process_function" to point to a function that will perform
    some action on CSP file and/or container objects
    '''
    def __init__(self,
                 path,
                 commit=False,
                 host='localhost',
                 passwd=None,
                 userid='administrator',
                 verbose=False):
        self.commit = commit
        self.errors = 0
        self.host = host
        self.indent = 0
        self.num_files = 0
        self.num_containers = 0
        self.num_objects = 0
        self.num_unknowns = 0
        self.passwd = passwd
        self.pathname = path
        self.path = path.split('/')
        self.processed = 0
        self.userid = userid
        self.verbose = verbose

        if (self.host.startswith('http:') or
            self.host.startswith('https:')):
            pass
        else:
            self.host = 'http://%s' % self.host

        headers = {}
        headers['Accept'] = FILE_LIST
        headers['X-Client-Specification'] = '3'
        headers['X-Cloud-Depth'] = '1'

        self.cloud = Cloud(self.userid, self.host, passwd=self.passwd)
        location = self.cloud.get_location_by_name(self.path[0])
        if not location:
            print 'Could not find root %s' % self.path[0]
            sys.exit(1)
        uri = location.get_root_container_uri()
        container = Container(self.cloud, url=uri)
        artist = self.path[2].replace('_', ' ')
        containers = []
        file_list = []
        total = 0
        #import pydevd; pydevd.settrace()
        for p in self.path[1:]:
            containers, file_list, total = container.get_contents()
            for container in containers:
                if container.get_name() in [p]:
                    if '_' in container.get_name():
                        newname = string.capwords(container.get_name().replace('_', ' '))
                        data = {'container': {'name': newname}}
                        if self.commit:
                            print 'Renaming container %s to %s' % (container.get_name(), newname)
                            rc = container.PUT(data)
                            if not rc:
                                print 'Could not rename container %s' % container.get_name()
                                self.errors += 1
                        else:
                            print 'Would rename container %s to %s' % (container.get_name(), newname)
                    break
            if container.get_name() not in [p]:
                print 'Could not find path %s' % self.pathname
                return

        containers, file_list, total = container.get_contents()
        print 'Found %d tracks in %s' % (len(file_list), self.pathname)
        for fileobj in file_list:
            newname = fileobj.get_name().replace('_', ' ')
            # newname = '0%s' % newname
            #newname = newname.replace("- various -", '-')
            #newname = newname.replace("Ric- Ky Martin Ricky Martin ", '')
            #newname = newname[5:]
            #newname = newname[:3] + '- ' + newname[3:]
            #if newname[1] == ' ':
            #    newname = '0%s' % newname
            ext = newname[-4:]
            n1 = newname[:-4]
            x = n1.split(' - ')
            try:
                newname = '%s - %s - %s%s' % (x[0], x[2], x[1], ext)
            except:
                pass
            #newname = newname.replace('- - ', '- ')
            newname = string.capwords(newname.replace('- %s -' % artist, '-'))
            data = {'file': {'mime_type': fileobj.get_mimetype(),
                             'name': newname}}
            print 'Renaming %s to %s' % (fileobj.get_name(), newname)
            if self.commit:
                if not fileobj.PUT(data):
                    print 'Could not process file %s' % obj.get_name()
                    self.errors += 1
                    continue
                self.processed += 1


def usage():
    '''
    Usage instructions.
    '''
    print ('Rename all tracks in an album.')
    print ('Customize the regex as needed.')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --userid=[userid] --passwd=[password] --path=[path]'
           % sys.argv[0])
    print ('')
    print (' Command Line options:')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - MCSP host url. Defaults to "localhost".')
    print ('  --userid    - MCSP userid. Defaults to "administrator".')
    print ('  --passwd    - MCSP userid password. If omitted, will be'
           '                prompted for.')
    print ('  --path      - path to the tracks to be renamed.')
    print ('  --verbose   - Print progress information.')
    print ('  --commit    - Commit changes. Default is only show what would be done.')
    print ('')

    sys.exit(0)

def main(argv):
    '''
    Main entry point
    '''
    if (len(argv) < 2):
        usage()
    commit = False
    host = 'localhost'
    userid = 'administrator'
    passwd = None
    path = None
    global verbose
    verbose = False

    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'host=',
                                    'commit',
                                    'nocommit',
                                    'userid=',
                                    'passwd=',
                                    'path=',
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
        elif opt == '--path':
            path = arg
        elif opt == '--commit':
            commit = True
        elif opt == '--nocommit':
            commit = False
        elif opt == '--passwd':
            passwd = arg
        elif opt == '--verbose':
            verbose = True

    if host is None or userid is None:
        usage()
        sys.exit(1)

    if passwd is None:
        passwd = getpass.getpass()

    pobj = ProcessObjects(path=path,
                          host=host,
                          commit=commit,
                          passwd=passwd,
                          userid=userid,
                          verbose=verbose)

    if pobj.errors > 0:
        print 'Encountered %d errors' % pobj.errors
    print 'Renamed %d tracks' % pobj.processed

if __name__ == '__main__':
    main(sys.argv[1:])
