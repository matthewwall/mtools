#!/usr/bin/env python
__version__ = '0.2.1'
"""Data collector/processor for EDS One-Wire Server

Copyright 2012 Matthew Wall, all rights reserved

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
A PARTICULAR PURPOSE.

See http://www.gnu.org/licenses/


Collect data from sensors attached to an EDS One-Wire to Ethernet server.  This
script queries an EDS device then saves data to database or uploads to any
number of hosted services.

Server Mode:

Configure the EDS device to client mode, sending data to the IP address and
port of the host on which this script is running.

Client Mode:

Configure the EDS device to server mode.  This script will request data at an
interval defined by the polling interval parameter.


Notes:
  Do not sample more often than every 5 seconds.
"""
__author__ = 'mwall'
__app__ = 'edsmon'

MINUTE = 60

# the field we use to uniquely identify the source of data
PKT_ID = 'devicename'

# if set to 1, print out what would be uploaded but do not do the upload.
SKIP_UPLOAD = 0

# if set to 1, obfuscate any serial number before uploading
OBFUSCATE_SERIALS = 1

# how often to poll the device, in seconds
DEFAULT_POLL_INTERVAL = 60

# size of the rolling buffer into which data are cached
# should be at least max(upload_period) / sample_period
# a sample period of 10s and max upload period of 15m (900s), with contingency
# of 5m (300s) server/network downtime yields a buffer of 120
DEFAULT_BUFFER_SIZE = 120

# how long to wait before considering an upload to have failed, in seconds
DEFAULT_UPLOAD_TIMEOUT = 15

# how often to upload data, in seconds
# this may be overridden by specific services
DEFAULT_UPLOAD_PERIOD = 15 * MINUTE


# ethernet settings
IP_SERVER_HOST = '' # bind to default
IP_SERVER_PORT = 8083
IP_CLIENT_PORT = 80
IP_CLIENT_TIMEOUT = 60
IP_DEFAULT_MODE = 'client'
IP_POLL_INTERVAL = DEFAULT_POLL_INTERVAL
IP_BUFFER_SIZE = 2048
IP_URL = '/details.xml'

# database defaults
DB_HOST = 'localhost'
DB_USER = 'edsuser'
DB_PASSWD = 'edspass'
DB_DATABASE = 'eds'
DB_TABLE = 'onewire'
DB_FILENAME = 'eds.db'   # filename for sqlite databases
DB_INSERT_PERIOD = MINUTE     # how often to record to database, in seconds
DB_POLL_INTERVAL = 60         # how often to poll the database, in seconds

# rrd defaults
RRD_DIR = 'rrd' # directory in which to put the rrd files
RRD_STEP = DEFAULT_POLL_INTERVAL # how often we get samples, in seconds
RRD_HEARTBEAT = 2 * RRD_STEP  # typically twice the step, in seconds
# 10s, 5m, 30m, 1h
# 4d at 10s, 60d at 5m, 365d at 30m, 730d at 1h
# 2087836 bytes per rrd file
RRD_STEPS = [1, 18, 180, 360]
RRD_RESOLUTIONS = [34560, 17280, 17520, 17520]
# 30s, 5m, 30m, 1h
# 4d at 30s, 60d at 5m, 365d at 30m, 730d at 1h
# 1534876 bytes per rrd file
#RRD_STEPS = [1,6,60,120]
#RRD_RESOLUTIONS = [11520, 17280, 17520, 17520]
RRD_UPDATE_PERIOD = 60  # how often to update the rrd files, in seconds
RRD_POLL_INTERVAL = 120 # how often to poll when rrd is source, in seconds

# smart energy groups defaults
#   http://smartenergygroups.com/api
# the map is a comma-delimited list of id,meter pairs.  for example:
#   6F0000042B82C428,living room,6E0000042C170828,parlor
SEG_URL = 'http://api.smartenergygroups.com/api_sites/stream'
SEG_UPLOAD_PERIOD = MINUTE
SEG_TIMEOUT = 15 # seconds
SEG_TOKEN = ''
SEG_MAP = ''

# thingspeak defaults
#   http://community.thingspeak.com/documentation/api/
#   Uploads are limited to no more than every 15 seconds per channel.
TS_URL = 'http://api.thingspeak.com/update'
TS_UPLOAD_PERIOD = MINUTE
TS_TIMEOUT = 15 # seconds
TS_TOKENS = ''
TS_FIELDS = ''

# pachube/cosm defaults
#   https://cosm.com/docs/v2/
PBE_URL = 'http://api.xively.com/v2/feeds'
PBE_UPLOAD_PERIOD = MINUTE
PBE_TIMEOUT = 15 # seconds
PBE_TOKEN = ''
PBE_FEED = ''

# open energy monitor emoncms defaults
OEM_URL = 'https://localhost/emoncms/api/post'
OEM_UPLOAD_PERIOD = MINUTE
OEM_TIMEOUT = 15 # seconds
OEM_TOKEN = ''
OEM_NODE = None


import base64
import bisect
import optparse
import socket
import os
import sys
import time
import traceback
import urllib2

try:
    import MySQLdb
except ImportError, e:
    MySQLdb = None

try:
    from sqlite3 import dbapi2 as sqlite
except ImportError, e:
    sqlite = None

try:
    import rrdtool
except ImportError, e:
    rrdtool = None

try:
    import cjson as json
    # XXX: maintain compatibility w/ json module
    setattr(json, 'dumps', json.encode)
    setattr(json, 'loads', json.decode)
except ImportError, e:
    try:
        import simplejson as json
    except ImportError, e:
        import json

try:
    import ConfigParser
except ImportError, e:
    ConfigParser = None

try:
    import xml.etree.cElementTree as etree
    etree_warn = False
except ImportError:
    etree_warn = True
    import xml.etree.ElementTree as etree


class ReadError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __repr__(self):
        return repr(self.msg)
    def __str__(self):
        return self.msg


# logging and error reporting
#
# note that setting the log level to debug will affect the application
# behavior, especially when sampling the serial line, as it changes the
# timing of read operations.
LOG_ERROR = 0
LOG_WARN  = 1
LOG_INFO  = 2
LOG_DEBUG = 3
LOGLEVEL  = 2

def dbgmsg(msg):
    if LOGLEVEL >= LOG_DEBUG:
        logmsg(msg)

def infmsg(msg):
    if LOGLEVEL >= LOG_INFO:
        logmsg(msg)

def wrnmsg(msg):
    if LOGLEVEL >= LOG_WARN:
        logmsg(msg)

def errmsg(msg):
    if LOGLEVEL >= LOG_ERROR:
        logmsg(msg)

def logmsg(msg):
    ts = fmttime(time.localtime())
    print "%s %s" % (ts, msg)

# Helper Functions

def fmttime(seconds):
    return time.strftime("%Y/%m/%d %H:%M:%S", seconds)

def mkts(seconds):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(seconds))

def getgmtime():
    return int(time.time())

def cleanvalue(s):
    """ensure that values read from configuration file are sane"""
    s = s.replace('\n', '')    # we never want newlines
    s = s.replace('\r', '')    # or carriage returns
    if s.lower() == 'false':
        s = False
    elif s.lower() == 'true':
        s = True
    return s

def pairs2dict(s):
    """convert comma-delimited name,value pairs to a dictionary"""
    items = s.split(',')
    m = dict()
    for k, v in zip(items[::2], items[1::2]):
        m[k] = v
    return m

def mklabel(serial, channel):
    return '%s_%s' % (serial, channel)

def mkfn(dir, label):
    label = label.replace(' ','_')
    label = label.replace(':','_')
    label = label.replace('/','_')
    return '%s/%s.rrd' % (dir, label)

def mktag(label, prefix):
    return '%s%s' % (prefix, label)

def mknode(label):
    label = label.replace(' ', '_')
    label = label.replace(':', '_')
    return label


class XMLPacket(object):
    def __init__(self):
        self.EDSURL = '{http://www.embeddeddatasystems.com/schema/owserver}'
        if etree_warn:
            wrnmsg('using non-native ElementTree implementation')

    def getchannels(self, packet):
        channels = []
        for label in packet.keys():
            if label.startswith('t_'):
                channels.append(label)
        return channels

    # i would prefer to do a pattern match for the element names, but i do not
    # yet understand how xpath works and there are too many xml parsing options
    # in python.  so just try to parse for the device name and use that to
    # determine whether we need to prefix the labels.  when the eds is a client
    # we do not need to prefix, when the eds is a server we do.
    def compile(self, xml):
        """convert a raw packet into a compiled packet"""
#        dbgmsg(xml)
        pkt = dict()
        pkt['time_created'] = getgmtime()
#        pkt['xml'] = xml
        root = etree.fromstring(xml)
        pfx = ''
        elem = root.find(mktag('DeviceName', pfx))
        if elem is None:
            pfx = self.EDSURL
        elem = root.find(mktag('DeviceName', pfx))
        if elem is None:
            raise ReadError('no device name found')
        pkt['devicename'] = elem.text
        pkt['hostname'] = root.findtext(mktag('HostName', pfx))
        pkt['macaddr'] = root.findtext(mktag('MACAddress', pfx))
        pkt['pollcount'] = int(root.findtext(mktag('PollCount', pfx)))
        thermos = root.findall(mktag('owd_DS18B20', pfx))
        for t in thermos:
            i = t.findtext(mktag('ROMId', pfx))
            v = t.findtext(mktag('Temperature', pfx))
            pkt['t_%s' % i] = float(v)
        return pkt

    def printPacket(self, p):
        ts = fmttime(time.localtime(p['time_created']))
        print ts + ': Device: %s' % p['devicename']
        print ts + ': Host: %s' % p['hostname']
        print ts + ': MAC: %s' % p['macaddr']
        print ts + ': Count: %d' % p['pollcount']
        for i in p.keys():
            if i.startswith('t_'):
                print ts + ': %s: %f' % (i, p[i])
#        print ts+': xml: %s' % p['xml']


# Schemas for storing in database

class BaseSchema(object):
    def gettablesql(self):
        """sql for creating the table that will store the data"""
        return None

    def insertdata(self, cursor, table, packet):
        """insert packet into database, return number of records"""
        return 0

class TemperatureSchema(BaseSchema):
    def __init__(self):
        self.name = 'TemperatureSchema'

    def gettablesql(self):
        schema = [('time_created', 'INTEGER NOT NULL'), # unix epoch
                  ('macaddr', 'VARCHAR(12)'),           # MAC address of device
                  ('sensor_id', 'VARCHAR(16)'),         # temperature sensor id
                  ('value', 'REAL')]                    # sensor value
        return ', '.join(["`%s` %s" % _type for _type in schema])

    def insertdata(self, cursor, table, packet):
        nrec = 0
        macaddr = packet['macaddr'].replace(':', '')
        for key in packet:
            if key.startswith('t_'):
                sql = "insert into %s (time_created, macaddr, sensor_id, value) values (%d, '%s', '%s', %.4f)" % (table, packet['time_created'], macaddr, key[2:], packet[key])
                dbgmsg('DB: %s' % sql)
                cursor.execute(sql)
                nrec += 1
        return nrec


# Data Collector classes
#
# all of the collectors are buffered - they contain an array of packets, sorted
# by timestamp and grouped by the device name of the eds device.
class BufferedDataCollector(object):
    def __init__(self):
        self.packet_buffer = CompoundBuffer(BUFFER_SIZE)

    # Read a single sample.  This should be overridden by derived classes.
    def _read(self):
        return []

    def setup(self):
        pass

    def cleanup(self):
        pass

    def read(self, packet_format):
        packets = self._read()
        for p in packets:
            p = packet_format.compile(p)
            self.packet_buffer.insert(p['time_created'], p)


# the client collector opens a socket, makes a request for data, retrieves the
# data, then closes the socket.
class SocketClientCollector(BufferedDataCollector):
    def __init__(self, host, port, poll_interval):
        if not host:
            print 'Socket Error: no host specified'
            sys.exit(1)

        super(SocketClientCollector, self).__init__()
        self._timeout = IP_CLIENT_TIMEOUT
        self._host = host
        self._port = int(port)
        self._poll_interval = int(poll_interval)
        self._first = True
        infmsg('SOCKET: timeout: %d' % self._timeout)
        infmsg('SOCKET: server host: %s' % self._host)
        infmsg('SOCKET: server port: %d' % self._port)
        infmsg('SOCKET: poll interval: %d' % self._poll_interval)
        self._url = 'http://%s:%d%s' % (self._host, self._port, IP_URL)
        infmsg('SOCKET: url: %s' % self._url)

    def read(self, packet_format):
        if not self._first:
            dbgmsg('SOCKET: waiting for %d seconds' % self._poll_interval)
            time.sleep(self._poll_interval)
        self._first = False
        BufferedDataCollector.read(self, packet_format)

    def _read(self):
        response = urllib2.urlopen(self._url, None, self._timeout)
        data = response.read()
        dbgmsg('SOCKET: got %d bytes' % len(data))
        return [data]


# the server collector opens a socket then blocks, waiting for connections from
# clients.  it expects http post requests.
class SocketServerCollector(BufferedDataCollector):
    def __init__(self, host, port):
        super(SocketServerCollector, self).__init__()
        self._host = host
        self._port = int(port)
        self._sock = None
        self._conn = None
        infmsg('SOCKET: bind host: %s' % self._host)
        infmsg('SOCKET: bind port: %d' % self._port)
        self.PAYLOAD_START_TAG = '<?xml'
        self.PAYLOAD_END_TAG = '</Devices-Detail-Response>'

    def setup(self):
        dbgmsg('SOCKET: binding to %s:%d' % (self._host, self._port))
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except: # REUSEPORT may not be supported on all systems  
            pass
        self._sock.bind((self._host, self._port))
        self._sock.listen(1)

    def cleanup(self):
        if self._sock:
            dbgmsg('SOCKET: closing socket')
            self._sock.close()
            self._sock = None

    def read(self, packet_format):
        try:
            dbgmsg('SOCKET: waiting for connection')
            self._conn, addr = self._sock.accept()
            BufferedDataCollector.read(self, packet_format)
        finally:
            if self._conn:
                dbgmsg('SOCKET: closing connection')
                try:
                    self._conn.shutdown(socket.SHUT_RD)
                except:
                    pass
                self._conn.close()
                self._conn = None

    def _read(self):
        data = []
        while 1:
            d = self._conn.recv(IP_BUFFER_SIZE)
            if not d:
                break
            data.append(d)
        s = ''.join(data)
        dbgmsg('received %d bytes' % len(str))
        idx = s.find(self.PAYLOAD_START_TAG)
        if idx >= 0:
            s = s[idx:]
            idx = s.find(self.PAYLOAD_END_TAG)
            if idx >= 0:
                s = s[0:idx + len(self.PAYLOAD_END_TAG)]
        else:
            s = ''
        return [s]


# Buffer Classes

class MovingBuffer(object):
    """Maintain fixed-size buffer of data.  Oldest packets are removed."""
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.packets = []

    def insert(self, timestamp, packet):
        dbgmsg('buffering packet ts:%d id:%s' % (timestamp, packet[PKT_ID]))
        bisect.insort(self.packets, (timestamp, packet))
        if len(self.packets) > self.maxsize:
            del(self.packets[0])

    def newest(self, timestamp):
        """return all packets with timestamp newer than specified timestamp"""
        idx = bisect.bisect(self.packets, (timestamp, {}))
        return self.packets[idx:]

    def oldest(self):
        """return the oldest packet in the buffer"""
        return self.packets[0]

    def size(self):
        return len(self.packets)

    def lastmod(self):
        if len(self.packets) > 0:
            return self.packets[len(self.packets)-1][0]
        return 0

class CompoundBuffer(object):
    """Variable number of moving buffers, each associated with an ID"""
    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.buffers = dict()
        dbgmsg('buffer size: %d' % self.maxsize)

    def insert(self, timestamp, packet):
        return self.getbuffer(packet[PKT_ID]).insert(timestamp, packet)

    def newest(self, serial, timestamp):
        return self.getbuffer(serial).newest(timestamp)

    def oldest(self, serial):
        return self.getbuffer(serial).oldest()

    def size(self, serial):
        return self.getbuffer(serial).size()

    def lastmod(self, serial):
        return self.getbuffer(serial).lastmod()

    def getbuffer(self, serial):
        if not serial in self.buffers:
            dbgmsg('adding buffer for %s' % serial)
            self.buffers[serial] = MovingBuffer(self.maxsize)
        return self.buffers[serial]

    def getkeys(self):
        return self.buffers.keys()


# Packet Processor Classes

class BaseProcessor(object):
    def __init__(self):
        self.last_processed = dict()
        self.process_period = 1 # in seconds

    def setup(self):
        pass

    def process_compiled(self, packet_buffer):
        now = getgmtime()
        for i in packet_buffer.getkeys():
            if packet_buffer.size(i) < 1:
                dbgmsg('buffer is empty for %s' % i)
                continue
            if (not i in self.last_processed
                or now >= self.last_processed[i] + self.process_period):
                if not i in self.last_processed:
                    ts = packet_buffer.oldest(i)[0]
                else:
                    ts = self.last_processed[i]
                data = packet_buffer.newest(i, ts)
                if len(data) > 0:
                    dbgmsg('%d buffered packets id:%s' % (len(data), i))
                    packets = []
                    for a in data:
                        self.last_processed[i] = a[0] + 1
                        packets.append(a[1])
                    self._process_compiled(packets)
                else:
                    dbgmsg('not enough data for %s' % i)
                    continue
            else:
                x = self.last_processed[i] + self.process_period - now
                dbgmsg('waiting %d seconds to process packets for %s' % (x, i))

    def _process_compiled(self, packets):
        pass

    def handle(self, exception):
        return False

    def cleanup(self):
        pass


class PrintProcessor(BaseProcessor):
    def __init__(self):
        super(PrintProcessor, self).__init__()

    def _process_compiled(self, packets):
        for p in packets:
            print
            PACKET_FORMAT.printPacket(p)


class RRDProcessor(BaseProcessor):
    def __init__(self, dir, step, heartbeat, period):
        if not rrdtool:
            print 'RRD Error: rrdtool module could not be imported.'
            sys.exit(1)

        super(RRDProcessor, self).__init__()
        self.process_period = int(period)
        self._dir = dir
        self._step = step
        self._heartbeat = heartbeat
        infmsg('RRD: update period: %d' % self.process_period)
        infmsg('RRD: dir: %s' % self._dir)
        infmsg('RRD: step: %s' % self._step)
        infmsg('RRD: heartbeat: %s' % self._heartbeat)

    def _mkdir(self):
        if not os.path.exists(self._dir):
            infmsg('RRD: creating rrd directory %s' % self._dir)
            os.makedirs(self._dir)

    def _rrdexists(self, packet, channel):
        fn = mkfn(self._dir, mklabel(packet[PKT_ID], channel))
        return os.path.exists(fn)

    # dstype is one of COUNTER, GAUGE, DERIVE, ABSOLUTE, COMPUTE
    def _create_rrd(self, packet, channel, dstype):
        self._mkdir()
        ts = packet['time_created'] - 1
        label = mklabel(packet[PKT_ID], channel)
        fn = mkfn(self._dir, label)
        infmsg('RRD: creating rrd file %s' % fn)
        rc = rrdtool.create(
            fn,
            '--step', str(self._step),
            '--start', str(ts),
            ["DS:data:%s:%d:U:U" % (dstype, self._heartbeat)],
            "RRA:AVERAGE:0.5:%d:%d" % (RRD_STEPS[0], RRD_RESOLUTIONS[0]),
            "RRA:AVERAGE:0.5:%d:%d" % (RRD_STEPS[1], RRD_RESOLUTIONS[1]),
            "RRA:AVERAGE:0.5:%d:%d" % (RRD_STEPS[2], RRD_RESOLUTIONS[2]),
            "RRA:AVERAGE:0.5:%d:%d" % (RRD_STEPS[3], RRD_RESOLUTIONS[3]))
        if rc:
            wrnmsg("RRD: failed to create '%s': %d" % (fn, rc))

    def _update_rrd(self, label, values):
        fn = mkfn(self._dir, label)
        infmsg('RRD: updating %s with %d values' % (fn, len(values)))
        rc = rrdtool.update(fn, values)
        if rc:
            wrnmsg("RRD: failed to update '%s': %d" % (fn, rc))

    def _getvalues(self, ts, value, dstype):
        if dstype == 'GAUGE':
            s = '%d:%f' % (ts, value)
        else:
            s = '%d:%d' % (ts, value)
        return s

    def _getchannels(self, packet):
        return PACKET_FORMAT.getchannels(packet)

    def _gettype(self, channel):
        return 'GAUGE'

    def _update_files(self, packets):
        values = dict()
        for p in packets:
            for x in self._getchannels(p):
                label = mklabel(p[PKT_ID], x)
                t = self._gettype(x)
                if not self._rrdexists(p, x):
                    self._create_rrd(p, x, t)
                if not label in values:
                    values[label] = []
                values[label].append(self._getvalues(p['time_created'], p[x], t))
        for label in values.keys():
            if len(values[label]) > 0:
                self._update_rrd(label, values[label])

    def _process_compiled(self, packets):
        self._process_all(packets)

    def _process_latest(self, packets):
        # process latest packet - assumes rrd update period is same as step
        if len(packets) > 0:
            self._update_files([packets[len(packets) - 1]])

    def _process_all(self, packets):
        # process each packet - assumes device emits at same frequency as step
        self._update_files(packets)


class DatabaseProcessor(BaseProcessor):
    def __init__(self, table, period, schema):
        super(DatabaseProcessor, self).__init__()
        self.db_schema = schema
        self.process_period = int(period)

    def _process_compiled(self, packets):
        for p in packets:
            cursor = self.conn.cursor()
            nrec = self.db_schema.insertdata(cursor, self.db_fqtable, p)
            cursor.close()
            infmsg('DB: inserted %d records' % nrec)
        self.conn.commit()


class MySQLClient(object):
    def __init__(self, host, user, passwd, database, table):
        if not MySQLdb:
            print 'MySQL Error: MySQLdb module could not be imported.'
            sys.exit(1)

        self.conn = None
        self.db_host = host
        self.db_user = user
        self.db_passwd = passwd
        self.db_database = database
        self.db_table = table
        self.db_fqtable = self.db_database + '.' + table

        infmsg('MYSQL: host: %s' % self.db_host)
        infmsg('MYSQL: username: %s' % self.db_user)
        infmsg('MYSQL: database: %s' % self.db_database)
        infmsg('MYSQL: table: %s' % self.db_table)

    def _open_connection(self):
        dbgmsg('MYSQL: establishing connection')
        self.conn = MySQLdb.connect(host=self.db_host,
                                    user=self.db_user,
                                    passwd=self.db_passwd,
                                    db=self.db_database)
    def _close_connection(self):
        if self.conn:
            dbgmsg('MYSQL: closing connection')
            self.conn.close()
            self.conn = None

    def setup(self):
        self._open_connection()

    def cleanup(self):
        self._close_connection()


class MySQLProcessor(DatabaseProcessor, MySQLClient):
    def __init__(self, host, user, passwd, database, table, period, schema):
        MySQLClient.__init__(self, host, user, passwd, database, table)
        DatabaseProcessor.__init__(self, self.db_fqtable, period, schema)
        infmsg('MYSQL: process_period: %d' % self.process_period)

    def handle(self, e):
        if type(e) == MySQLdb.Error:
            errmsg('MySQL Error: [#%d] %s' % (e.args[0], e.args[1]))
            return True
        return super(MySQLProcessor, self).handle(e)

    def setup(self):
        try:
            cfg = MySQLConfigurator(self.db_host, self.db_user, self.db_passwd, self.db_database, self.db_table, self.db_schema)
            cfg.configure()
        except Exception:
            pass
#        MySQLClient.setup(self)

    def cleanup(self):
        pass
#        MySQLClient.cleanup(self)

    def _process_compiled(self, packets):
        MySQLClient._open_connection(self)
        DatabaseProcessor._process_compiled(self, packets)
        MySQLClient._close_connection(self)


class MySQLConfigurator(MySQLClient):
    def __init__(self, host, user, passwd, database, table, schema):
        MySQLClient.__init__(self, host, user, passwd, database, table)
        self.db_schema = schema

    def setup(self):
        self.conn = MySQLdb.connect(host=self.db_host,
                                    user=self.db_user,
                                    passwd=self.db_passwd)

    def configure(self):
        try:
            self.setup()

            infmsg('MYSQL: creating database %s' % self.db_database)
            cursor = self.conn.cursor()
            sql = 'create database if not exists %s' % self.db_database
            cursor.execute(sql)
            cursor.close()

            infmsg('MYSQL: creating table %s' % self.db_fqtable)
            cursor = self.conn.cursor()
            sql = 'create table if not exists %s (%s)' % (self.db_fqtable, self.db_schema.gettablesql())
            cursor.execute(sql)
            cursor.close()

            self.conn.commit()

        finally:
            self.cleanup()


class UploadProcessor(BaseProcessor):
    class FakeResult(object):
        def geturl(self):
            return 'fake result url'
        def info(self):
            return 'fake result info'
        def read(self):
            return 'fake result read'

    def __init__(self):
        super(UploadProcessor, self).__init__()
        self.process_period = DEFAULT_UPLOAD_PERIOD
        self.timeout = DEFAULT_UPLOAD_TIMEOUT
        self.urlopener = None
        pass

    def setup(self):
        pass

    def _process_compiled(self, packets):
        pass

    def handle(self, exception):
        return False

    def cleanup(self):
        pass

    def _create_request(self, url):
        req = urllib2.Request(url)
        req.add_header("User-Agent", "%s/%s" % (__app__, __version__))
        return req

    def _urlopen(self, url, data):
        result = None
        try:
            req = self._create_request(url)
            dbgmsg('%s: url: %s\n  headers: %s\n  data: %s' %
                   (self.__class__.__name__, req.get_full_url(), req.headers, data))
            if SKIP_UPLOAD:
                result = UploadProcessor.FakeResult()
            elif self.urlopener:
                result = self.urlopener.open(req, data, self.timeout)
            else:
                result = urllib2.urlopen(req, data, self.timeout)
            infmsg('%s: %dB url, %dB payload' %
                   (self.__class__.__name__, len(url), len(data)))
            dbgmsg('%s: url: %s\n  response: %s' %
                   (self.__class__.__name__, result.geturl(), result.info()))
        except urllib2.HTTPError, e:
            self._handle_urlopen_error(e, url, data)
            errmsg('%s Error: %s' % (self.__class__.__name__, e.read()))
        except Exception, e:
            errmsg('%s Error: %s' % (self.__class__.__name__, e))
        return result

    def _handle_urlopen_error(self, e, url, data):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  URL:  ' + url,
                        '\n  data: ' + data]))


# smart energy groups expects delta measurements for both power and energy.
# this is not a cumulative energy reading!
class SmartEnergyGroupsProcessor(UploadProcessor):
    def __init__(self, url, token, map_str, period, timeout):
        super(SmartEnergyGroupsProcessor, self).__init__()
        self.url = url
        self.token = token
        self.map_str = map_str
        self.process_period = int(period)
        self.timeout = int(timeout)
        self.map = dict()

        infmsg('SEG: upload period: %d' % self.process_period)
        infmsg('SEG: url: %s' % self.url)
        infmsg('SEG: token: %s' % self.token)
        infmsg('SEG: map: %s' % self.map_str)

    def setup(self):
        if not (self.url and self.token):
            print 'SmartEnergyGroups Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.token:
                print '  No token'
            sys.exit(1)
        self.map = pairs2dict(self.map_str.lower())
        self.urlopener = urllib2.build_opener(urllib2.HTTPHandler)

    def _process_compiled(self, packets):
        nodes = []
        for p in packets:
            s = []
            if self.map:
                for idx, c in enumerate(PACKET_FORMAT.getchannels(p)):
                    key = c
                    if key in self.map:
                        meter = self.map[key] or c
                        s.append('(%s %.2f)' % (meter, p[c]))
            else:
                for idx, c in enumerate(PACKET_FORMAT.getchannels(p)):
                    meter = c
                    s.append('(%s %.2f)' % (meter, p[c]))
            if len(s):
                ts = mkts(p['time_created'])
                node = mknode(p[PKT_ID])
                s.insert(0, '(node %s %s ' % (node, ts))
                s.append(')')
                nodes.append(''.join(s))
        if len(nodes):
            nodes.insert(0, 'data_post=(site %s ' % self.token)
            nodes.append(')')
            result = self._urlopen(self.url, ''.join(nodes))
            if result and result.read:
                resp = result.read()
                resp = resp.replace('\n', '')
                if not resp == '(status ok)':
                    wrnmsg('SEG: upload failed: %s' % resp)

    def _handle_urlopen_error(self, e, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload]))

    def _create_request(self, url):
        req = super(SmartEnergyGroupsProcessor, self)._create_request(url)
        req.get_method = lambda: 'PUT'
        return req


class ThingSpeakProcessor(UploadProcessor):
    def __init__(self, url, tokens, fields, period, timeout):
        super(ThingSpeakProcessor, self).__init__()
        self.url = url
        self.tokens_str = tokens
        self.fields_str = fields
        self.process_period = int(period)
        self.timeout = int(timeout)
        self.tokens = dict()
        self.fields = dict()

        infmsg('TS: upload period: %d' % self.process_period)
        infmsg('TS: url: %s' % self.url)
        infmsg('TS: tokens: %s' % self.tokens_str)
        infmsg('TS: fields: %s' % self.fields_str)

    def setup(self):
        if not (self.url and self.tokens_str):
            print 'ThingSpeak Error: Insufficient parameters'
            if not self.url:
                print '  No URL'
            if not self.tokens_str:
                print '  No tokens'
            sys.exit(1)
        self.tokens = pairs2dict(self.tokens_str)
        self.fields = pairs2dict(self.fields_str)

    def _process_compiled(self, packets):
        for p in packets:
            devid = p[PKT_ID]
            if devid in self.tokens:
                token = self.tokens[devid]
                s = []
                for idx, c in enumerate(PACKET_FORMAT.getchannels(p)):
                    key = mklabel(devid, c)
                    if not self.fields:
                        s.append('&field%d=%.2f' % (idx+1, p[c]))
                    elif key in self.fields:
                        s.append('&field%s=%.2f' % (self.fields[key], p[c]))
                if len(s):
                    s.insert(0, 'key=%s' % token)
                    s.insert(1, '&datetime=%s' % mkts(p['time_created']))
                    result = self._urlopen(self.url, ''.join(s))
                    if result and result.read:
                        resp = result.read()
                        if resp == 0:
                            wrnmsg('TS: upload failed for %s: %s' % (devid, resp))                        
                    else:
                        wrnmsg('TS: upload failed for %s' % devid)
            else:
                wrnmsg('TS: no token defined for %s' % devid)


class PachubeProcessor(UploadProcessor):
    def __init__(self, url, token, feed, period, timeout):
        super(PachubeProcessor, self).__init__()
        self.url = url
        self.token = token
        self.feed = feed
        self.process_period = int(period)
        self.timeout = int(timeout)

        infmsg('PBE: upload period: %d' % self.process_period)
        infmsg('PBE: url: %s' % self.url)
        infmsg('PBE: token: %s' % self.token)
        infmsg('PBE: feed: %s' % self.feed)

    def setup(self):
        if not (self.url and self.token and self.feed):
            print 'Pachube Error: Insufficient parameters'
            if not self.url:
                print '  A URL is required'
            if not self.url:
                print '  A token is required'
            if not self.feed:
                print '  A feed is required'
            sys.exit(1)

    def _process_compiled(self, packets):
        streams = dict()
        for p in packets:
            devid = p[PKT_ID]
            ts = mkts(p['time_created'])
            for idx, c in enumerate(PACKET_FORMAT.getchannels(p)):
                dskey = mklabel(devid, c)
                if not dskey in streams:
                    streams[dskey] = {'id': dskey, 'datapoints': []}
                dp = {'at': ts, 'value': p[c]}
                streams[dskey]['datapoints'].append(dp)
        if len(streams.keys()) > 0:
            data = {'version': '1.0.0', 'datastreams': []}
            for key in streams.keys():
                data['datastreams'].append(streams[key])
            url = '%s/%s' % (self.url, self.feed)
            result = self._urlopen(url, json.dumps(data))
            # FIXME: need error handling here

    def _create_request(self, url):
        req = super(PachubeProcessor, self)._create_request(url)
        req.add_header('X-PachubeApiKey', self.token)
        req.get_method = lambda: 'PUT'
        return req

    def _handle_urlopen_error(self, e, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload]))


class OpenEnergyMonitorProcessor(UploadProcessor):
    def __init__(self, url, token, node, period, timeout):
        super(OpenEnergyMonitorProcessor, self).__init__()
        self.url = url
        self.token = token
        self.node = node
        self.process_period = int(period)
        self.timeout = int(timeout)

        infmsg('OEM: upload period: %d' % self.process_period)
        infmsg('OEM: timeout: %d' % self.timeout)
        infmsg('OEM: url: %s' % self.url)
        infmsg('OEM: token: %s' % self.token)
        infmsg('OEM: node: %s' % self.node)

    def setup(self):
        if not (self.url and self.token):
            print 'OpenEnergyMonitor Error: Insufficient parameters'
            if not self.url:
                print '  A URL is required'
            if not self.token:
                print '  A token is required'
            sys.exit(1)

    def _process_compiled(self, packets):
        for p in packets:
            devid = p[PKT_ID]
            data = []
            for idx, c in enumerate(PACKET_FORMAT.getchannels(p)):
                data.append('%s:%.2f' % (mklabel(devid, c), p[c]))
            if len(data):
                nstr = '' if self.node is None else '&node=%s' % self.node
                url = '%s?apikey=%s&time=%s%s&json={%s}' % (
                    self.url, self.token, p['time_created'], nstr,
                    ','.join(data))
                result = self._urlopen(url, '')
                # FIXME: need error handling here

    def _create_request(self, url):
        req = super(OpenEnergyMonitorProcessor, self)._create_request(url)
        return req

    def _handle_urlopen_error(self, e, url, payload):
        errmsg(''.join(['%s Error: %s' % (self.__class__.__name__, e),
                        '\n  URL:   ' + url,
                        '\n  token: ' + self.token,
                        '\n  data:  ' + payload]))


# The monitor contains the application control logic, tying together the
# communications mechanism with the data collection and data processing.

class Monitor(object):
    def __init__(self, packet_collector, packet_processors):
        self.packet_collector = packet_collector
        self.packet_processors = packet_processors
        dbgmsg('packet format is %s' % PACKET_FORMAT.__class__.__name__)
        dbgmsg('using collector %s' % packet_collector.__class__.__name__)
        dbgmsg('using %d processors:' % len(self.packet_processors))
        for p in self.packet_processors:
            dbgmsg('  %s' % p.__class__.__name__)

    def read(self):
        self.packet_collector.read(PACKET_FORMAT)

    def process(self):
        dbgmsg('buffer info:')
        for i in self.packet_collector.packet_buffer.getkeys():
            dbgmsg('  %s: %3d of %3d (%d)' % (
                    i,
                    self.packet_collector.packet_buffer.size(i),
                    self.packet_collector.packet_buffer.maxsize,
                    self.packet_collector.packet_buffer.lastmod(i)))
        for p in self.packet_processors:
            try:
                dbgmsg('processing with %s' % p.__class__.__name__)
                p.process_compiled(self.packet_collector.packet_buffer)
            except Exception, e:
                if not p.handle(e):
                    wrnmsg('Exception in %s: %s' % (p.__class__.__name__, e))
                    if LOGLEVEL >= LOG_DEBUG:
                        traceback.print_exc()

    # Loop forever, break only for keyboard interrupts.
    def run(self):
        try:
            dbgmsg('setup %s' % self.packet_collector.__class__.__name__)
            self.packet_collector.setup()
            for p in self.packet_processors:
                dbgmsg('setup %s' % p.__class__.__name__)
                p.setup()

            while True:
                try:
                    self.read()
                    self.process()
                except KeyboardInterrupt, e:
                    raise e
                except Exception, e:
                    errmsg(e)
                    if LOGLEVEL >= LOG_DEBUG:
                        traceback.print_exc()

        except KeyboardInterrupt:
            sys.exit(0)
        except Exception, e:
            errmsg(e)
            if LOGLEVEL >= LOG_DEBUG:
                traceback.print_exc()
            sys.exit(1)

        finally:
            for p in self.packet_processors:
                dbgmsg('cleanup %s' % p.__class__.__name__)
                p.cleanup()
            dbgmsg('cleanup %s' % self.packet_collector.__class__.__name__)
            self.packet_collector.cleanup()


if __name__ == '__main__':
    parser = optparse.OptionParser(version=__version__)

    parser.add_option('-c', '--config-file', dest='configfile', help='read configuration from FILE', metavar='FILE')
    parser.add_option('-p', '--print', action='store_true', dest='print_out', default=False, help='print data to screen')
    parser.add_option('-q', '--quiet', action='store_true', dest='quiet', default=False, help='quiet output')
    parser.add_option('-v', '--verbose', action='store_false', dest='quiet', default=False, help='verbose output')
    parser.add_option('--debug', action='store_true', default=False, help='debug output')
    parser.add_option('--skip-upload', action='store_true', default=False, help='do not upload data but print what would happen')
    parser.add_option('--buffer-size', help='number of packets to keep in cache', metavar='SIZE')

    group = optparse.OptionGroup(parser, 'database setup options')
    group.add_option('--mysql-config', action='store_true', default=False, help='configure mysql database')
    group.add_option('--sqlite-config', action='store_true', default=False, help='configure sqlite database')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'tcp/ip source options')
    group.add_option('--ip', action='store_true', dest='ip_read', default=False, help='read from tcp/ip source such as WIZnet or EtherBee')
    group.add_option('--ip-host', help='ip host', metavar='HOSTNAME')
    group.add_option('--ip-port', help='ip port', metavar='PORT')
    group.add_option('--ip-mode', help='act as client or server', metavar='MODE')
    group.add_option('--ip-poll-interval', help='for client mode, how often to poll the device for data, 0 indicates block for data; default is 0', metavar='PERIOD')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'mysql source options')
    group.add_option('--mysql-src', action='store_true', dest='mysql_read', default=False, help='read from mysql database')
    group.add_option('--mysql-src-host', help='source database host', metavar='HOSTNAME')
    group.add_option('--mysql-src-user', help='source database user', metavar='USERNAME')
    group.add_option('--mysql-src-passwd', help='source database password', metavar='PASSWORD')
    group.add_option('--mysql-src-database', help='source database name', metavar='DATABASE')
    group.add_option('--mysql-src-table', help='source database table', metavar='TABLE')
    group.add_option('--mysql-poll-interval', help='how often to poll the database in seconds', metavar='PERIOD')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'sqlite source options')
    group.add_option('--sqlite-src', action='store_true', dest='sqlite_read', default=False, help='read from sqlite database')
    group.add_option('--sqlite-src-file', help='source database file', metavar='FILE')
    group.add_option('--sqlite-src-table', help='source database table', metavar='TABLE')
    group.add_option('--sqlite-poll-interval', help='how often to poll the database in seconds', metavar='PERIOD')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'mysql options')
    group.add_option('--mysql', action='store_true', dest='mysql_out', default=False, help='write data to mysql database')
    group.add_option('--mysql-host', help='database host', metavar='HOSTNAME')
    group.add_option('--mysql-user', help='database user', metavar='USERNAME')
    group.add_option('--mysql-passwd', help='database password', metavar='PASSWORD')
    group.add_option('--mysql-database', help='database name', metavar='DATABASE')
    group.add_option('--mysql-table', help='database table', metavar='TABLE')
    group.add_option('--mysql-insert-period', help='database insert period in seconds', metavar='PERIOD')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'sqlite options')
    group.add_option('--sqlite', action='store_true', dest='sqlite_out', default=False, help='write data to sqlite database')
    group.add_option('--sqlite-file', help='database filename', metavar='FILE')
    group.add_option('--sqlite-table', help='database table', metavar='TABLE')
    group.add_option('--sqlite-insert-period', help='database insert period in seconds', metavar='PERIOD')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'rrd options')
    group.add_option('--rrd', action='store_true', dest='rrd_out', default=False, help='write data to round-robin database')
    group.add_option('--rrd-dir', help='directory for rrd files', metavar='DIR')
    group.add_option('--rrd-step', help='step size in seconds', metavar='STEP')
    group.add_option('--rrd-heartbeat', help='heartbeat in seconds', metavar='HEARTBEAT')
    group.add_option('--rrd-update-period', help='update period in seconds', metavar='PERIOD')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Smart Energy Groups options')
    group.add_option('--smartenergygroups', action='store_true', dest='smartenergygroups_out', default=False, help='upload data using SmartEnergyGroups API')
    group.add_option('--seg-token', help='token', metavar='TOKEN')
    group.add_option('--seg-url', help='URL', metavar='URL')
    group.add_option('--seg-map', help='channel-to-device mapping', metavar='MAP')
    group.add_option('--seg-upload-period', help='upload period in seconds', metavar='PERIOD')
    group.add_option('--seg-timeout', help='timeout period in seconds', metavar='TIMEOUT')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'ThingSpeak options')
    group.add_option('--thingspeak', action='store_true', dest='thingspeak_out', default=False, help='upload data using ThingSpeak API')
    group.add_option('--ts-url', help='URL', metavar='URL')
    group.add_option('--ts-tokens', help='ECM-to-ID/token mapping', metavar='TOKENS')
    group.add_option('--ts-fields', help='channel-to-field mapping', metavar='FIELDS')
    group.add_option('--ts-upload-period', help='upload period in seconds', metavar='PERIOD')
    group.add_option('--ts-timeout', help='timeout period in seconds', metavar='TIMEOUT')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'Pachube options')
    group.add_option('--pachube', action='store_true', dest='pachube_out', default=False, help='upload data using Pachube API')
    group.add_option('--pbe-url', help='URL', metavar='URL')
    group.add_option('--pbe-token', help='token', metavar='TOKEN')
    group.add_option('--pbe-feed', help='feed', metavar='FEED')
    group.add_option('--pbe-upload-period', help='upload period in seconds', metavar='PERIOD')
    group.add_option('--pbe-timeout', help='timeout period in seconds', metavar='TIMEOUT')
    parser.add_option_group(group)

    group = optparse.OptionGroup(parser, 'OpenEnergyMonitor options')
    group.add_option('--oem', action='store_true', dest='oem_out', default=False, help='upload data using OpenEnergyMonitor API')
    group.add_option('--oem-url', help='URL', metavar='URL')
    group.add_option('--oem-token', help='token', metavar='TOKEN')
    group.add_option('--oem-node', help='node', metavar='NODE')
    group.add_option('--oem-upload-period', help='upload period in seconds', metavar='PERIOD')
    group.add_option('--oem-timeout', help='timeout period in seconds', metavar='TIMEOUT')
    parser.add_option_group(group)

    (options, args) = parser.parse_args()

    if options.quiet:
        LOGLEVEL = LOG_ERROR
    if options.debug:
        LOGLEVEL = LOG_DEBUG

    # if there is a configration file, read the parameters from file and set
    # values on the options object.
    if options.configfile:
        if not ConfigParser:
            print 'ConfigParser not loaded, cannot parse config file'
            sys.exit(1)
        config = ConfigParser.ConfigParser()
        try:
            config.read(options.configfile)
            for section in config.sections(): # section names do not matter
                for name, value in config.items(section):
                    if not getattr(options, name):
                        setattr(options, name, cleanvalue(value))
        except AttributeError, e:
            print 'unknown parameter in config file: %s' % e
            sys.exit(1)
        except Exception, e:
            print e
            sys.exit(1)

    if options.skip_upload:
        SKIP_UPLOAD = 1

    if not options.buffer_size:
        options.buffer_size = DEFAULT_BUFFER_SIZE
    BUFFER_SIZE = int(options.buffer_size)

    PACKET_FORMAT = XMLPacket()

    SCHEMA = TemperatureSchema()

    # Database Setup
    # run the database configurator then exit
    if options.mysql_config:
        db = MySQLConfigurator(options.mysql_host or DB_HOST,
                               options.mysql_user or DB_USER,
                               options.mysql_passwd or DB_PASSWD,
                               options.mysql_database or DB_DATABASE,
                               options.mysql_table or DB_TABLE,
                               SCHEMA)
        db.configure()
        sys.exit(0)
    if options.sqlite_config:
        db = SqliteConfigurator(options.sqlite_file or DB_FILENAME,
                                options.sqlite_table or DB_TABLE)
        db.configure()
        sys.exit(0)

    # Data Collector setup
    if options.ip_read:
        if (options.ip_mode and not
            (options.ip_mode == 'client' or options.ip_mode == 'server')):
            print 'Unknown mode %s: use client or server' % options.ip_mode
            sys.exit(1)

        mode = options.ip_mode or IP_DEFAULT_MODE

        if mode == 'server':
            col = SocketServerCollector(options.ip_host or IP_SERVER_HOST,
                                        options.ip_port or IP_SERVER_PORT)
        else:
            col = SocketClientCollector(options.ip_host,
                                        options.ip_port or IP_CLIENT_PORT,
                                        options.ip_poll_interval or IP_POLL_INTERVAL)

    elif options.mysql_read:
        col = MySQLCollector(options.mysql_src_host or DB_HOST,
                             options.mysql_src_user or DB_USER,
                             options.mysql_src_passwd or DB_PASSWD,
                             options.mysql_src_database or DB_DATABASE,
                             options.mysql_src_table or DB_TABLE,
                             options.mysql_poll_interval or DB_POLL_INTERVAL)

    elif options.sqlite_read:
        col = SqliteCollector(options.sqlite_src_file or DB_FILENAME,
                              options.sqlite_src_table or DB_TABLE,
                              options.sqlite_poll_interval or DB_POLL_INTERVAL)

    else:
        print 'Please specify a data source (or \'-h\' for help):'
        print '  --ip          read from tcp/ip socket'
        print '  --mysql-src   read from mysql database'
        print '  --sqlite-src  read from sqlite database'
        sys.exit(1)

    # Packet Processor Setup
    if not (options.print_out or options.mysql_out or options.sqlite_out
            or options.rrd_out or options.oem_out or options.pachube_out
            or options.smartenergygroups_out or options.thingspeak_out):
        print 'Please specify one or more processing options (or \'-h\' for help):'
        print '  --print              print to screen'
        print '  --mysql              write to mysql database'
        print '  --sqlite             write to sqlite database'
        print '  --rrd                write to round-robin database'
        print '  --oem                upload to OpenEnergyMonitor'
        print '  --pachube            upload to Pachube'
        print '  --smartenergygroups  upload to SmartEnergyGroups'
        print '  --thingspeak         upload to ThingSpeak'
        sys.exit(1)

    procs = []

    if options.print_out:
        procs.append(PrintProcessor())
    if options.mysql_out:
        procs.append(MySQLProcessor
                     (options.mysql_host or DB_HOST,
                      options.mysql_user or DB_USER,
                      options.mysql_passwd or DB_PASSWD,
                      options.mysql_database or DB_DATABASE,
                      options.mysql_table or DB_TABLE,
                      options.mysql_insert_period or DB_INSERT_PERIOD,
                      SCHEMA))
    if options.sqlite_out:
        procs.append(SqliteProcessor
                     (options.sqlite_file or DB_FILENAME,
                      options.sqlite_table or DB_TABLE,
                      options.sqlite_insert_period or DB_INSERT_PERIOD))
    if options.rrd_out:
        procs.append(RRDProcessor
                     (options.rrd_dir or RRD_DIR,
                      options.rrd_step or RRD_STEP,
                      options.rrd_heartbeat or RRD_HEARTBEAT,
                      options.rrd_update_period or RRD_UPDATE_PERIOD))
    if options.smartenergygroups_out:
        procs.append(SmartEnergyGroupsProcessor
                     (options.seg_url or SEG_URL,
                      options.seg_token or SEG_TOKEN,
                      options.seg_map or SEG_MAP,
                      options.seg_upload_period or SEG_UPLOAD_PERIOD,
                      options.seg_timeout or SEG_TIMEOUT))
    if options.thingspeak_out:
        procs.append(ThingSpeakProcessor
                     (options.ts_url or TS_URL,
                      options.ts_tokens or TS_TOKENS,
                      options.ts_fields or TS_FIELDS,
                      options.ts_upload_period or TS_UPLOAD_PERIOD,
                      options.ts_timeout or TS_TIMEOUT))
    if options.pachube_out:
        procs.append(PachubeProcessor
                     (options.pbe_url or PBE_URL,
                      options.pbe_token or PBE_TOKEN,
                      options.pbe_feed or PBE_FEED,
                      options.pbe_upload_period or PBE_UPLOAD_PERIOD,
                      options.pbe_timeout or PBE_TIMEOUT))
    if options.oem_out:
        procs.append(OpenEnergyMonitorProcessor
                     (options.oem_url or OEM_URL,
                      options.oem_token or OEM_TOKEN,
                      options.oem_node or OEM_NODE,
                      options.oem_upload_period or OEM_UPLOAD_PERIOD,
                      options.oem_timeout or OEM_TIMEOUT))

    mon = Monitor(col, procs)
    mon.run()

    sys.exit(0)
