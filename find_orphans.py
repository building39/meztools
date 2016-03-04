#!/usr/bin/env python
'''
Created on Sep 24, 2013

@author: mmartin
'''
from base64 import (
    b64encode
)
import getopt
import getpass
import httplib
import json
import os
import requests
import sys

userid = 'administrator'
passwd = None
BUCKET = 'mstore'
host = 'http://localhost'
port = 8098
STORAGEROOT = '/cdmi/storage_root'
VERSION = '1.0.0'


def find_orphans():
    '''
    Get a list of all container and file keys
    '''
    # import pydevd; pydevd.settrace()
    bucket = BUCKET
    conn = httplib.HTTPConnection('%s:%s' % (host, port))
    url = os.path.join('/riak', bucket) + '?keys=stream'
    print 'URL: %s' % url
    print 'Host: %s' % host
    print 'Port: %d' % port
    conn.request('GET', url)
    response = conn.getresponse()
    processdata(response)
    conn.close()

def GET(url, headers=None):
    res = requests.get(url=url,
                       allow_redirects=True,
                       headers=headers)
    if res.status_code in [200]:
        return (res.status_code, json.loads(res.text))
    else:
        return (res.status_code, None)

def _process(keys):
    keylist = []
    objtype = None
    auth_basic = "Basic %s" % b64encode("%s:%s" %
                        (userid, passwd))
    headers = {'Authorization': auth_basic,
               'X-Cloud-Depth': '0',
               'X-Client-Specification': '3'}
    for key in keys:
        if len(key) == 33:
            if key.startswith('c'):
                objtype = 'containers'
                headers['Accept'] = 'application/vnd.csp.container-info+json'
            elif key.startswith('f'):
                objtype = 'files'
                headers['Accept'] = 'application/vnd.csp.file-info+json'
            else:
                continue
            url = 'http://%s/v2/%s/%s' % (host, objtype, b64encode(key))
            _status, response = GET(url, headers)
            if not response:
                continue
            if 'administrator' == response[objtype[:-1]]['owner']:
                continue  # we're not interested in system objects
            keylist.append((key,
                            response[objtype[:-1]]['name'],
                            response[objtype[:-1]]['owner'],
                            response[objtype[:-1]]['parent']))

    return keylist

def processdata(response):
    keylist = []
    data = response.read()
    while data:
        s = data.find('{"keys":[')
        if s == -1:
            data = response.read()
            continue
        data = data[s:]
        end = data.find(']}')
        if end == -1:  # spanning a block of data
            tempdata = response.read()
            if not tempdata:
                print 'Short read!!! %s' % data
                sys.exit(1)
            data = '%s%s' % (data, tempdata)

            end = data.find(']}')
        keys = data[:end + 2]
        keydict = json.loads(keys)
        keylist.extend(_process(keydict['keys']))
        data = data[end + 2:]
        print 'retrieved %d keys\r' % len(keylist),
        sys.stdout.flush()
    auth_basic = "Basic %s" % b64encode("%s:%s" % (userid, passwd))
    headers = {'Authorization': auth_basic,
               'X-Cloud-Depth': '0',
               'Accept': 'application/vnd.csp.container-info+json',
               'X-Client-Specification': '3'}
    errors = 0
    orphans = 0
    healthy = 0
    for key, name, owner, parent in keylist:
        if key.startswith('c') or key.startswith('f'):
            pass
        else:
            print 'Key %s skipped, unknown type' % key
            continue
        url = parent['uri']
        status, response = GET(url, headers)
        if status in [200]:
            healthy += 1
        elif status in [404]:
            status = try_other_keys(key)
            if status in [200]:
                healthy += 1
            else:
                print 'No parent found for key %s' % key
                print '    Object name: %s' % name
                print '    Object owner: %s' % owner
                print '    Object parent: %s' % parent['uri']
                orphans += 1
        else:
            print 'Unexpected response status: %d' % status
            print '    Object key %s' % key
            print '    Object name: %s' % name
            print '    Object owner: %s' % owner
            print '    Object parent: %s' % parent['uri']
            errors += 1
    print '%d healthy objects' % healthy
    print '%d orphaned objects' % orphans
    print '%d object errors' % errors

def try_other_keys(key):
    '''
    This is a total hack to get around a server bug.
    See jira ticket MEZEO-540

    TODO: remove this function once the server is fixed.
    '''
    url = 'http://%s:%s/riak/%s/%s' % (host, port, BUCKET, key)
    status, data = GET(url)
    if status not in [200]:
        print 'fatal error %d retrieving file object from riak' % status
        sys.exit(1)
    pkeys = data['p']
    pkeys = pkeys.split(' ')
    for pkey in pkeys:
        url = 'http://%s:%s/riak/%s/%s' % (host, port, BUCKET, pkey)
        status, data = GET(url)
        if status in [200]:
            if 'DeleteMarker' not in data['vcn']:
                return status

def usage():
    '''
    Usage instructions.
    '''
    print ('CDMI to CSP')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --host=[hostname] --userid=[userid] --passwd=[password] --port=[port]'
           % sys.argv[0])
    print ('')
    print (' Command Line options:')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - MCSP and riak hostname.')
    print ('                Defaults to "localhost".')
    print ('  --userid    - MCSP Administrator userid. Required.')
    print ('                Defaults to "administrator".')
    print ('  --passwd    - MCSP Administrator password.')
    print ('                If not present, will be prompted for.')
    print ('  --port      - Riak http port number.')
    print ('                Defaults to 8098.')
    print ('')

    sys.exit(0)

def main(argv):
    '''
    Main entry point
    '''
    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'host=',
                                    'userid=',
                                    'passwd=',
                                    'port='])
    except getopt.GetoptError, e:
        print ('opt error %s' % e)
        print ('')
        usage()

    global host
    global userid
    global passwd
    global port

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
        elif opt == '--host':
            host = arg
        elif opt == '--userid':
            userid = arg
        elif opt == '--passwd':
            passwd = arg
        elif opt == '--port':
            port = arg

    if passwd is None:
        passwd = getpass.getpass()

    find_orphans()

if __name__ == '__main__':
    main(sys.argv[1:])
