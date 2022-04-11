#!/bin/env python3
"""
qsstv to Cloudlog QSO uploader.
This script reads QSO events from IPC events of qsstv and uploads the ADIF records to Cloudlog.
No non-standard library packages are required to run this.
usage: wsjt_cloudlog.py [-h] [--verbose] url api_key station_id
positional arguments:
  url                   URL for CloudLog.
  api_key               CloudLog API key.
  station_id            CloudLog station ID to upload QSO to.
optional arguments:
  -h, --help            show this help message and exit
  --verbose             Output debugging information.
"""
import json
import logging
import socket
import struct
import sys
import urllib.request
from functools import partial
import sysv_ipc
import re

BUFF_SIZE = 16
SIZEOF_FLOAT = 8


def unpack_with_offset(format, data, offset):
    """
    Unpack the format from the data and increment the offset
    """
    delta_offset = struct.calcsize(format)
    unpacked = struct.unpack_from(format, data, offset=offset)
    if len(unpacked) == 1:
        unpacked = unpacked[0]
    return unpacked, offset + delta_offset


def unpack_wsjt_utf8(data, offset):
    """
    The wireformat uses a utf8 sub-format which is a uint32 number followed by
    that number of bytes of utf8 encoded string.
    """
    n_bytes, offset = unpack_with_offset(">I", data, offset)
    utf_bytes, offset = unpack_with_offset(f">{int(n_bytes)}s", data, offset)
    return utf_bytes.decode('utf8'), offset


def parse_header(data, offset):
    # First parse the magic number to verify the message is one we want to parse
    try:
        magic, _ = unpack_with_offset(">I", data, offset)
        assert magic == MAGIC_NUMBER
    except Exception:
        log.exception("Unable to parse message in WSJT format")
        return None, offset
    header_format = ">III"  # Note we have put the type in the header for dispatch parsing.
    return unpack_with_offset(header_format, data, offset)

def parse_heartbeat(data, offset):
    heartbeat_id, offset = unpack_wsjt_utf8(data, offset)
    max_schema, offset = unpack_with_offset(">I", data, offset)
    version, offset = unpack_wsjt_utf8(data, offset)
    revision, offset = unpack_wsjt_utf8(data, offset)
    return (heartbeat_id, max_schema, version, revision), offset

def parse_logged_adif(data, offset):
    unique_id, offset = unpack_wsjt_utf8(data, offset)
    adif_content, offset = unpack_wsjt_utf8(data, offset)
    return (unique_id, adif_content), offset


def parse_qsstv_message(data, mtype): #, callbacks=None):
    log.info(f"Got message with type {mtype}")
    message = data.decode('ascii')
    # strip last two chars "\x01\x00"
    message = message[:-2]
    log.info("Raw message: %s" % data.decode('ascii'))
    pieces = re.split(r'\x01', message)
    #log.info("Raw message: %s" % pieces[0])
    for piece in pieces:
        tmp = piece.split(':')
        if tmp[1] == "":
            tmp[1] = 'n/a'
        log.info("%s: %s" % (tmp[0], tmp[1]))

"""
Cloudlog
"""


def test_cloudlog(base_url):
    """
    Check that we can make a request to the given cloudlog URL.
    """
    response = urllib.request.urlopen(f"{base_url}/index.php/api/statistics")
    assert response.code == 201
    data = json.loads(response.read().decode())
    if 'Today' not in data:
        log.warning("Unknown response from Cloudlog %s. May not be connected correctly.", data)
    return data


def upload_to_cloudlog(base_url, api_key, station_id, payload):
    adif = payload[1]
    # Split out the record from the header
    adif = adif.split("<EOH>")[1].strip()
    data = {
        "key": api_key,
        "station_profile_id": station_id,
        "type":"adif",
        "string": adif
    }
    jsondata = json.dumps(data).encode('utf-8')

    req = urllib.request.Request(f"{base_url}/index.php/api/qso")
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    try:
        response = urllib.request.urlopen(req, jsondata)
        log.info("Sent QSO to cloudlog at %s (station ID: %s), got response %s", base_url, station_id, response.read().decode())
    except Exception:
        log.exception("Failed to send ADIF to cloudlog")


if __name__ == "__main__":
    """
    Run the script
    """
    # Parse the arguments
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('url', metavar='url', type=str,
                        help='URL for CloudLog.')
    parser.add_argument('api_key', metavar='api_key', type=str,
                        help='CloudLog API key.')
    parser.add_argument('station_id', metavar='station_id', type=str,
                        help='CloudLog station ID to upload QSO to.')
    parser.add_argument('--verbose', action="store_true",
                        help='Output debugging information.')
    arguments = vars(parser.parse_args())

    # Setup logger
    log = logging.getLogger(__name__)
    log.setLevel("INFO")
    if arguments["verbose"]:
        log.setLevel("DEBUG")
    stream_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    # Test connection to cloudlog
    try:
        test_cloudlog(arguments['url'])
    except Exception:
        log.exception("Unable to connect to Cloudlog")
        sys.exit(1)
    log.info("Successfully tested connection to Cloudlog")


    # Define functions which are called after certain types of messages are decoded
    callbacks = {
        #0: lambda payload: log.info("Got heartbeat from %s version %s %s", payload[0], payload[2], payload[3]),
        12: partial(upload_to_cloudlog, arguments["url"], arguments["api_key"], arguments["station_id"]),
    }

    # Listen for IPC messages from qsstv
    log.info("Waiting for qsstv IPC events")
    #message = b'program:QSSTV 9\x01version:1\x01date:5 Apr 2022\x01time:1420\x01endTime:1420\x01call:\x01mhz:144.21509\x01mode:SSTV\x01tx:\x01rx:\x01name:\x01qth:\x01state:\x01province:\x01country:\x01locator:\x01serialout:\x01serialin:\x01free1:\x01notes:\x01power:\x01\x00'
    #parse_qsstv_message(message, 88)
    #sys.exit()

try:
    mq = sysv_ipc.MessageQueue(1238, sysv_ipc.IPC_CREAT)

    while True:
        message, mtype = mq.receive()
        log.info("New IPC message received")
        #print(f"Raw message: {message}")
        parse_qsstv_message(message, mtype) #, callbacks)
        #print("TEST: %s", mtype)

except sysv_ipc.ExistentialError:
    print("ERROR: message queue creation failed")


