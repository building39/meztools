#!/usr/bin/env python
'''
Created on Oct 4, 2013

@author: mmartin

Parse output of dump_bucket into a sqlite database.

'''

import getopt
import json
import os
import re
import sqlite3
import sys

COMMIT_THRESHOLD = 1000
DBNAME = 'riak_sqlized.db'
VALUE_SEP = re.compile(r",|}|]")
VERSION = '1.0.0'


class SQLize(object):

    def __init__(self,
                 dumpfile,
                 dbpath=None,
                 host=None,
                 bucket=None,
                 port=8098,
                 verbose=False):
        self._data = ''
        self.dumpfd = None
        self.jsondoc = {}
        self.dumpfile = dumpfile
        self.host = host,
        self.bucket = bucket
        self.port = port
        self.table_list = []
        self.verbose = verbose
        self.commit_threshold = 0
        if dbpath is None:
            self.dbpath = './%s' % DBNAME
        else:
            # TODO: make this option fool-proof.
            self.dbpath = dbpath
        if os.path.exists(self.dbpath):
            os.remove(self.dbpath)
        self.connection = sqlite3.Connection(self.dbpath)

    def process_dumpfile(self):
        '''
        Process input from dump_bucket.py
        '''
        import pydevd
        # pydevd.settrace()
        num_docs = 0
        jsondoc = ''
        try:
            with open(self.dumpfile, 'r') as self.dumpfd:
                while True:
                    key = self._get_objkey()
                    if key is None:
                        break  # all done
                    try:
                        self._data = ''
                        self._get_data()

                        try:
                            jsondoc = json.loads(self.buf)
                            num_docs += 1
                            print 'Processed document #%d' % num_docs
                        except:
                            print 'Bad JSON data.'
                            print 'buffer contents:\n%s\n\n' % self.buf
                        if num_docs == 42104:
                            pass  # artificial break point
                        # TODO: Insert this data into the sqlite database
                        rc = self.insert(jsondoc)
                        print 'Clean JSON: \n%s' % self.jsondoc
                    except:
                        print 'json failure with data: %s' % self._data
                        print 'data is type %s' % type(self._data)
                        raise
                    # print 'Key: %s' % key
                    # print 'Data: %s' % data
        except:
            print 'Could not access file "%s"' % self.dumpfile
            print 'Aborting...'
            raise
            sys.exit(1)
        finally:
            print "Found %d docs" % num_docs
            self.connection.close()

    def process_riak(self):
        '''
        Process input from riak bucket
        TODO: Implement this some day.
        '''
        print 'Oops! This functionality is yet to be implemented. Please'
        print 'create a dump file with dump_bucket.py and supply the path to'
        print 'that file in the --dumpfile= command line option.'
        sys.exit(1)

    def _clean_up_json(self):
        while len(self._data) > 0:
            self._data = self._data.lstrip()
            if self._data[0] == "'":
                self._data = self._data.split("'")[1]
            if self._data[0] == '{':
                self._process_dict()
            elif self._data[0] == '[':
                self._process_list()

    def _commit(self):
        self.commit_threshold += 1
        if self.commit_threshold > COMMIT_THRESHOLD:
            self.connection.commit()
            self.commit_threshold = 0

    def create_stmt(self, doc):
        stmt = 'CREATE TABLE %s(' % doc['cn']
        for key in doc:
            stmt = '%s%s TEXT, ' % (stmt, key)
        stmt = '%s)' % stmt[:-2]
        return stmt

    def _get_data(self):
        for line in self.dumpfd:
            line = line.strip()
            if line.startswith('data:'):
                break
        self._get_json()

    def _get_json(self):
        self.buf = ''
        for line in self.dumpfd:
            if line.startswith('--------'):
                break
            self._data += line.lstrip().rstrip()  # just the data, ma'am
        print "Processing %s" % self._data
        self._clean_up_json()

    def _get_key(self, key):
        try:
            if key[0] == '"' and key[len(key) - 1] == '"':
                key = key.split('"')[1]
            else:
                key = key.split("'")[1]
        except:
            print 'Key error!'
        return key

    def _get_objkey(self):
        line = ''
        for line in self.dumpfd:
            line = line.strip()
            if line.startswith('key (orig):'):
                break
        if len(line) > 0:
            return line.split(':')[1].lstrip().rstrip()
        else:
            return None

    def _get_value(self):
        self._data = self._data.lstrip()
        savedata = self._data
        if self._data.startswith("u''"):  # an empty string
            value = '""'
            self._data = self._data[3:]
        elif self._data.startswith("u'"):  # a unicode string
            value = '"%s"' % self._data.split("'")[1]
            self._data = self._data[(len(value) + 1):]
            if value.startswith('""'):
                value = '%s%s' % ('"\\"', value[2:])
            if value.endswith('""'):
                value = '%s%s' % (value[:len(value) - len('""')], '\\""')
        elif self._data.startswith('"'):
            _, value, self._data = self._data.split('"', 2)
            value = '"%s"' % value
        else:
            value, self._data = VALUE_SEP.split(self._data, 1)
            self._data = savedata[len(value):].strip()  # put delimiter back
            if value in ['None', 'True', 'False']:
                value = '"%s"' % value
            else:
                value = value.replace('""', '"\\"')
        value = value.replace('\\x', '\\\\x')
        return value

    def insert(self, doc):
        if 'cn' in doc:
            table_name = doc['cn']
        else:
            return
        cur = self.connection.cursor()
        if table_name not in self.table_list:
            self.table_list.append(table_name)
            stmt = self.create_stmt(doc)
            cur.execute("DROP TABLE IF EXISTS %s" % table_name)
            try:
                cur.execute(stmt)
            except:
                e = sys.exc_info()[0]
                print 'SQL CREATE TABLE ERROR: %s' % e
                print 'SQL STMT: %s' % stmt
        stmt = self.insert_stmt(doc)
        try:
            cur.execute(stmt)
        except:
            e = sys.exc_info()[0]
            print 'SQL INSERT ERROR: %s' % e
            print 'SQL STMT: %s' % stmt
        self._commit()

    def insert_stmt(self, doc):
        stmt = 'INSERT INTO %s VALUES(' % doc['cn']
        for key in doc:
            value = doc[key]
            if type(value) is unicode:
                value = value.encode('ascii', 'ignore')
            else:
                value = str(value)
            value = value.replace("'", '"')
            stmt = "%s'%s', " % (stmt, value)
        stmt = '%s)' % stmt[:-2]
        return stmt

    def _process_dict(self):
        try:
            self._data = self._data.lstrip()
        except:
            pass
        self.buf = '%s {' % self.buf
        self._data = self._data[1:].lstrip()
        while True:
            self._data = self._data.lstrip()  # get rid of leading whitespace
            if len(self._data) == 0:
                break
            if self._data[0] == '}':
                self.buf = '%s}' % self.buf.lstrip()
                if self._data.startswith("}'"):
                    self._data = self._data[2:]  # edge case
                else:
                    self._data = self._data[1:]  # eat the delimiter
                break
            elif self._data[0] == ',':
                self.buf = '%s,' % self.buf
                self._data = self._data[1:]
                continue
            key, self._data = self._data.split(':', 1)
            self.buf = '%s "%s": ' % (self.buf, self._get_key(key))
            if key in ['"events"']:
                pass
            self._data = self._data.lstrip()  # get rid of leading whitespace
            if self._data[0] == '[':  # value is a list
                self._process_list()
            elif self._data.startswith("u'["):  # edge case
                self._data = self._data[2:]
                self._process_list()
                if self._data.startswith("'"):
                    self._data = self._data[1:]
            elif self._data[0] == '{':  # value is a dictionary
                self._process_dict()
            elif self._data.startswith("u'{"):  # edge case
                self._data = self._data[2:]
                self._process_dict()
                if self._data.startswith("',"):
                    self._data = self._data[1:]
            else:  # value is an atom
                value = self._get_value()
                if self._data[0] == '}':  # end of this dictionary
                    self.buf = '%s %s}' % (self.buf, value)
                    self._data = self._data[1:]  # eat the delimiter
                    if len(self._data) > 0:
                        if self._data[0] == "'":  # edge case
                            self._data = self._data[1:]
                    break  # only way out of this method
                else:
                    self.buf = '%s %s ' % (self.buf, value)
        return

    def _process_list(self):
        self.buf = '%s [' % self.buf
        self._data = self._data[1:]
        while True:
            self._data = self._data.lstrip()
            if self._data[0] == '[':  # value is a list
                self._process_list()
            elif self._data[0] == ',':
                self.buf = '%s,' % self.buf
                self._data = self._data[1:]
            elif self._data[0] == '{':  # value is a dictionary
                self._process_dict()
            elif self._data[0] == ']':  # end of list
                self.buf = '%s]' % self.buf
                self._data = self._data[1:].lstrip()
                break  # this is the only way out of this method
            else:  # value is an atom
                value = self._get_value()
                if self._data.startswith(']'):  # end of this list
                    self.buf = '%s %s]' % (self.buf, value)
                    self._data = self._data[1:].lstrip()
                    break  # this is the only way out of this method
                else:
                    self.buf = '%s %s' % (self.buf, value)


def usage():
    print ('Parse riak objects into sqlite database')
    print ('Version : %s' % VERSION)
    print ('')
    print ('Usage: '
           '%s --dumpfile=<filename> [--host=<hostname>] [--port=<port number]'
           % sys.argv[0])
    print ('          [--bucket=<bucketname>]')
    print ('')
    print (' Command Line options:')
    print ('  --help      - Print this enlightening message')
    print ('  --host      - Riak host url. Optional.')
    print ('                To be implemented later.')
    print ('  --port      - Riak listening port. Optional. Default: 8098')
    print ('                To be implemented later.')
    print ('  --bucket    - Riak bucket to read. Optional.')
    print ('                To be implemented later.')
    print ('  --dumpfile  - Path to file output from dump_bucket. Required.')
    print ('  --dbpath    - Path to the database that will be created.')
    print ('                Optional. Default: ./%s') % DBNAME
    print ('  --verbose   - Print progress information.')
    print ('')

    sys.exit(0)


def main(argv):

    if (len(sys.argv) < 2):
        usage()

    bucket = None
    dumpfile = None
    host = None
    port = None
    verbose = False
    # import pydevd;pydevd.settrace()
    try:
        opts, _args = getopt.getopt(argv,
                                   '',
                                   ['help',
                                    'debug',
                                    'dumpfile=',
                                    'host=',
                                    'bucket=',
                                    'port=',
                                    'verbose'])
    except getopt.GetoptError:
        usage()

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
        elif opt == '--debug':
            global DEBUG
            DEBUG = True
        elif opt == '--dumpfile':
            dumpfile = arg
        elif opt == '--host':
            host = arg
        elif opt == '--bucket':
            bucket = arg
        elif opt == '--port':
            port = arg
        elif opt == '--verbose':
                verbose = True

    sqlize = SQLize(dumpfile,  # User's Path to dump file
                    host,  # Riak host url
                    bucket,  # Name of Riak bucket to be processed.
                    port,  # Port that Riak listens on.
                    verbose=verbose)  # print verbose information on progress

    if dumpfile:
        sqlize.process_dumpfile()
    elif host and bucket:
        sqlize.process_riak()
    else:
        print 'You must supply either a path to a dump file created by'
        print 'dump_bucket.py or a Riak host name and bucket name (with an'
        print 'optional port number).'
        usage()
        sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
