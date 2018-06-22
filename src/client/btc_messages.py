# pylint: disable=C0302
# FIXME ^^^ this file is too long
# Copyright (C) 2018, Bloxroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Different types of messages for the Bitcoin wire protocol.
#
import hashlib
import random
import socket
import struct

from btc_exceptions import *
from utils import *

magic_dict = {
    'main': 0xD9B4BEF9,
    'testnet': 0xDAB5BFFA,
    'testnet3': 0x0709110B,
    'regtest': 0xDAB5BFFA,
    'namecoin': 0xFEB4BEF9
}


def get_magic_number(networkname):
    return magic_dict[networkname]


sha256 = hashlib.sha256

# The length of everything in the header minus the checksum
BCH_HEADER_MINUS_CHECKSUM = 20
BCH_HDR_COMMON_OFF = 24
BLOCK_HDR_SIZE = 81
# Length of a sha256 hash
HASH_LEN = 32

# The services that we provide
# 1: can ask for full blocks.
# 0x20: Node that is compatible with the hard fork.
NODE_CASH = 0x20  # Bitcoin cash service bit
NODE_SERVICES = 1
NODE_CASH_SERVICES = 33

OBJTYPE_TX = 1
OBJTYPE_BLOCK = 2
OBJTYPE_FILTERED_BLOCK = 3


class BTCMessage(object):
    def __init__(self, magic=None, command=None, payload_len=None, buf=None):
        self.buf = buf
        self._memoryview = memoryview(buf)

        magic_num = magic if magic not in magic_dict else magic_dict[magic]
        checksum = sha256(
            sha256(self._memoryview[BCH_HDR_COMMON_OFF:payload_len + BCH_HDR_COMMON_OFF]).digest()).digest()

        off = 0
        struct.pack_into('<L12sL', buf, off, magic_num, command, payload_len)
        off += 20
        buf[off:off + 4] = checksum[0:4]

        self._magic = magic_num
        self._command = command
        self._payload_len = payload_len
        self._payload = None

    # Returns a memoryview of the message.
    def rawbytes(self):
        if self._payload_len is None:
            self._payload_len = struct.unpack_from('<L', self.buf, 16)[0]

        if self._payload_len + BCH_HDR_COMMON_OFF == len(self.buf):
            return self._memoryview
        else:
            return self._memoryview[0:self._payload_len + BCH_HDR_COMMON_OFF]

    # peek at message, return (is_a_full_message, magic, command, length)
    # input_buffer is an instance of InputBuffer
    @staticmethod
    def peek_message(input_buffer):
        buf = input_buffer.peek_message(BCH_HEADER_MINUS_CHECKSUM)
        if len(buf) < BCH_HEADER_MINUS_CHECKSUM:
            return False, None, None

        _magic, _command, _payload_len = struct.unpack_from('<L12sL', buf, 0)
        _command = _command.rstrip('\x00')
        if _payload_len <= input_buffer.length - 24:
            return True, _command, _payload_len
        return False, _command, _payload_len

    # parse a full message
    @staticmethod
    def parse(buf):
        if len(buf) < BCH_HEADER_MINUS_CHECKSUM:
            return None

        _magic, _command, _payload_len = struct.unpack_from('<L12sL', buf, 0)
        _command = _command.rstrip('\x00')
        _checksum = buf[20:24]

        if _payload_len != len(buf) - 24:
            log_err("Payload length does not match buffer size! Payload is %d. Buffer is %d bytes long" % (
                _payload_len, len(buf)))
            raise PayloadLenError(
                "Payload length does not match buffer size! Payload is %d. Buffer is %d bytes long" % (
                    _payload_len, len(buf)))

        if _checksum != sha256(sha256(
                buf[BCH_HDR_COMMON_OFF:_payload_len + BCH_HDR_COMMON_OFF]).digest()).digest()[0:4]:
            log_err("Checksum for packet doesn't match!")
            raise ChecksumError("Checksum for packet doesn't match! ", "Raw data: %s" % repr(buf))

        if _command not in message_types:
            raise UnrecognizedCommandError("%s message not recognized!" % (_command,), "Raw data: %s" % (repr(buf),))

        cls = message_types[_command]
        msg = cls(buf=buf)
        msg._magic, msg._command, msg._payload_len, msg._checksum = _magic, _command, _payload_len, _checksum
        return msg

    def magic(self):
        return self._magic

    def command(self):
        return self._command

    def payload_len(self):
        if self._payload_len is None:
            self._payload_len = struct.unpack_from('<L', self.buf, 16)[0]
        return self._payload_len

    def checksum(self):
        raise RuntimeError('FIXME')

        # FIXME checksum is undefined
        # return self._checksum

    def payload(self):
        if self._payload is None:
            self._payload = self.buf[BCH_HDR_COMMON_OFF:self.payload_len() + BCH_HDR_COMMON_OFF]
        return self._payload

    @staticmethod
    def get_header_from_partial_buf(buf):
        return struct.unpack_from('Q12s<I', buf, 0)


def ipaddrport_to_btcbytearray(ip_addr, ip_port):
    try:
        buf = bytearray(18)
        buf[10] = 0xff
        buf[11] = 0xff
        buf[12:16] = socket.inet_pton(socket.AF_INET, ip_addr)
        buf[16] = ((ip_port >> 8) & 0xFF)
        buf[17] = (ip_port & 0xFF)
    except socket.error:
        try:
            buf = bytearray(18)
            buf[0:16] = socket.inet_pton(socket.AF_INET6, ip_addr)
            buf[16] = ((ip_port >> 8) & 0xFF)
            buf[17] = (ip_port & 0xFF)
        except socket.error:
            return None
    return buf


ipv4prefix = bytearray(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff')


def btcbytearray_to_ipaddrport(btcbytearray):
    if btcbytearray[0:12] == ipv4prefix[0:12]:
        return socket.inet_ntop(socket.AF_INET, buffer(btcbytearray[12:16])), (btcbytearray[16] << 8) | btcbytearray[17]
    else:
        return socket.inet_ntop(socket.AF_INET6, buffer(btcbytearray[:16])), (btcbytearray[16] << 8) | btcbytearray[17]


# pack the value into a varint, return how many bytes it took
def pack_int_to_btcvarint(val, buf, off):
    if val < 253:
        struct.pack_into('B', buf, off, val)
        return 1
    elif val <= 65535:
        struct.pack_into('<BH', buf, off, 0xfd, val)
        return 3
    elif val <= 4294967295:
        struct.pack_into('<BI', buf, off, 0xfe, val)
        return 5
    else:
        struct.pack_into('<BQ', buf, off, 0xff, val)
        return 9


def get_sizeof_btcvarint(val):
    if val < 253:
        return 1
    elif val <= 65535:
        return 3
    elif val <= 4294967295:
        return 5
    else:
        return 9


# Buf must be a bytearray
def btcvarint_to_int(buf, off):
    assert isinstance(buf, bytearray)

    if buf[off] == 0xff:
        return struct.unpack_from('<Q', buf, off + 1)[0], 9
    elif buf[off] == 0xfe:
        return struct.unpack_from('<I', buf, off + 1)[0], 5
    elif buf[off] == 0xfd:
        return struct.unpack_from('<H', buf, off + 1)[0], 3
    else:
        return struct.unpack_from('B', buf, off)[0], 1


class VersionBTCMessage(BTCMessage):
    def __init__(self, magic=None, version=None, dst_ip=None, dst_port=None,
                 src_ip=None, src_port=None, nonce=None, start_height=None, user_agent=None, services=NODE_SERVICES,
                 buf=None):

        if buf is None:
            buf = bytearray(109 + len(user_agent))
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            struct.pack_into('<IQQQ', buf, off, version, services, int(time.time()), 1)
            off += 28
            buf[off:off + 18] = ipaddrport_to_btcbytearray(dst_ip, dst_port)
            off += 18
            struct.pack_into('<Q', buf, off, 1)
            off += 8
            buf[off:off + 18] = ipaddrport_to_btcbytearray(src_ip, src_port)
            off += 18
            struct.pack_into('<Q%dpL' % (len(user_agent) + 1,), buf, off, nonce, user_agent, start_height)
            off += 8 + len(user_agent) + 1 + 4
            # we do not send or parse the relay boolean. if present, we benignly ignore it.

            BTCMessage.__init__(self, magic, 'version', off - BCH_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._memoryview = memoryview(buf)
        self._version = self._services = self._timestamp = self._dst_ip = self._dst_port = None
        self._src_ip = self._src_port = self._nonce = self._start_height = self._user_agent = None

    def version(self):
        if self._version is None:
            off = BCH_HDR_COMMON_OFF
            self._version, self._services, self._timestamp = struct.unpack_from('<IQQ', self.buf, off)

        return self._version

    def services(self):
        if self._services is None:
            self.version()

        return self._services

    def timestamp(self):
        if self._version is None:
            self.version()
        return self._timestamp

    def dst_ip(self):
        if self._dst_ip is None:
            dst_ip_off = BCH_HDR_COMMON_OFF + 28
            self._dst_ip, self._dst_port = btcbytearray_to_ipaddrport(self.buf[dst_ip_off:dst_ip_off + 18])
        return self._dst_ip

    def dst_port(self):
        if self._dst_port is None:
            self.dst_ip()
        return self._dst_port

    def src_ip(self):
        if self._src_ip is None:
            src_ip_off = BCH_HDR_COMMON_OFF + 54
            self._src_ip, self._src_port = btcbytearray_to_ipaddrport(self.buf[src_ip_off:src_ip_off + 18])
        return self._src_ip

    def src_port(self):
        if self._src_port is None:
            self.src_ip()
        return self._src_port

    def nonce(self):
        if self._nonce is None:
            nonce_off = BCH_HDR_COMMON_OFF + 72
            self._nonce = struct.unpack_from('<Q', self.buf, nonce_off)[0]
        return self._nonce

    def user_agent(self):
        if self._user_agent is None:
            user_agent_off = BCH_HDR_COMMON_OFF + 80
            self._user_agent = struct.unpack_from('%dp' % (len(self.buf) - user_agent_off,), self.buf, user_agent_off)[
                0]
            height_off = user_agent_off + len(self._user_agent) + 1
            self._start_height = struct.unpack_from('<L', self.buf, height_off)[0]
        return self._user_agent

    def start_height(self):
        if self._user_agent is None:
            self.user_agent()
        return self._start_height


class VerAckBTCMessage(BTCMessage):
    def __init__(self, magic=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF)
            self.buf = buf

            BTCMessage.__init__(self, magic, 'verack', 0, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None


class PingBTCMessage(BTCMessage):
    def __init__(self, magic=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 8)
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            struct.pack_into('<Q', buf, off, random.randint(0, sys.maxint))
            off += 8

            BTCMessage.__init__(self, magic, 'ping', off - BCH_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._nonce = None

    def nonce(self):
        if self._nonce is None:
            if len(self.buf) == BCH_HDR_COMMON_OFF:
                self._nonce = -1
            elif len(self.buf) == BCH_HDR_COMMON_OFF + 4:
                self._nonce = struct.unpack_from('<L', self.buf, BCH_HDR_COMMON_OFF)[0]
            else:
                self._nonce = struct.unpack_from('<Q', self.buf, BCH_HDR_COMMON_OFF)[0]
        return self._nonce


class PongBTCMessage(BTCMessage):
    def __init__(self, magic=None, nonce=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 8)
            self.buf = buf

            off = BCH_HDR_COMMON_OFF

            if nonce != -1:
                struct.pack_into('<Q', buf, off, nonce)
                off += 8

            BTCMessage.__init__(self, magic, 'pong', off - BCH_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._nonce = None

    def nonce(self):
        if self._nonce is None:
            if len(self.buf) == BCH_HDR_COMMON_OFF:
                self._nonce = -1
            else:
                self._nonce = struct.unpack_from('<Q', self.buf, BCH_HDR_COMMON_OFF)[0]
        return self._nonce


class GetAddrBTCMessage(BTCMessage):
    def __init__(self, magic=None, buf=None):
        if buf is None:
            # We only construct empty getaddr messages.
            buf = bytearray(BCH_HDR_COMMON_OFF)
            self.buf = buf

            BTCMessage.__init__(self, magic, 'getaddr', 0, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None


# the addr argument should be an array of (timestamp, ipaddr, port) triples
class AddrBTCMessage(BTCMessage):
    # FIXME addrs arg is sharing global state
    def __init__(self, magic=None, addrs=[], buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 9 + len(addrs) * (4 + 18))
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            off += pack_int_to_btcvarint(len(addrs), buf, off)

            for triplet in addrs:
                # pack the timestamp
                struct.pack_into('<L', buf, off, triplet[0])
                off += 4
                # pack the host ip and port pair
                buf[off:off + 18] = ipaddrport_to_btcbytearray(triplet[1], triplet[2])
                off += 18

            BTCMessage.__init__(self, magic, 'addr', off - BCH_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

    def __iter__(self):
        raise RuntimeError('FIXME')
        # FIXME buf is not defined, change to self.buf and test
        # off = BCH_HDR_COMMON_OFF
        # count, size = btcvarint_to_int(buf, off)
        # off += size
        #
        # for i in xrange(count):
        #     timestamp = struct.unpack_from('<L', self.buf, off)
        #     off += 4
        #     host, port = btcbytearray_to_ipaddrport(buf[off:off+18])
        #     off += 18
        #     yield (timestamp, host, port)


# an inv_vect is an array of (type, hash) tuples
class InventoryBTCMessages(BTCMessage):
    def __init__(self, magic=None, inv_vect=None, command=None, buf=None):
        if buf is None:
            buf = bytearray(
                BCH_HDR_COMMON_OFF + 9 + 36 * len(inv_vect))  # we conservatively allocate the max space for varint
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            off += pack_int_to_btcvarint(len(inv_vect), buf, off)

            for inv_item in inv_vect:
                struct.pack_into('<L', buf, off, inv_item[0])
                off += 4
                buf[off:off + 32] = inv_item[1].get_big_endian()
                off += 32

            BTCMessage.__init__(self, magic, command, off - BCH_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

    def count(self):
        off = BCH_HDR_COMMON_OFF
        num_items, size = btcvarint_to_int(self.buf, off)
        return num_items

    def __iter__(self):
        off = BCH_HDR_COMMON_OFF
        num_items, size = btcvarint_to_int(self.buf, off)
        off += size
        self._num_items = num_items

        self._items = list()
        for _ in xrange(num_items):
            invtype = struct.unpack_from('<L', self.buf, off)[0]
            off += 4
            yield (invtype, BTCObjectHash(buf=self.buf, offset=off, length=HASH_LEN))
            off += 32


class InvBTCMessage(InventoryBTCMessages):
    def __init__(self, magic=None, inv_vects=None, buf=None):
        InventoryBTCMessages.__init__(self, magic, inv_vects, 'inv', buf)


class GetDataBTCMessage(InventoryBTCMessages):
    def __init__(self, magic=None, inv_vects=None, buf=None):
        InventoryBTCMessages.__init__(self, magic, inv_vects, 'getdata', buf)


class NotFoundBTCMessage(InventoryBTCMessages):
    def __init__(self, magic=None, inv_vects=None, buf=None):
        InventoryBTCMessages.__init__(self, magic, inv_vects, 'notfound', buf)


class DataBTCMessage(BTCMessage):
    # FIXME hashes is sharing global state
    def __init__(self, magic=None, version=None, hashes=[],
                 hash_stop=None, command=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 9 + (len(hashes) + 1) * 32)
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            struct.pack_into('<I', buf, off, version)
            off += 4
            off += pack_int_to_btcvarint(len(hashes), buf, off)

            for hash_val in hashes:
                buf[off:off + 32] = hash_val.get_big_endian()
                off += 32

            buf[off:off + 32] = hash_stop.get_big_endian()
            off += 32

            BTCMessage.__init__(self, magic, command, off - BCH_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._hash_count = self._hashes = self._hash_stop = None

    def version(self):
        if self._version is None:
            self._version = struct.unpack_from('<I', self.buf, BCH_HDR_COMMON_OFF)
        return self._version

    def hash_count(self):
        if self._hash_count is None:
            raise RuntimeError('FIXME')
            # FIXME buf is not defined, should be self.buf, fix and test
            # off = BCH_HDR_COMMON_OFF + 4
            # self._hash_count, size = btcvarint_to_int(buf, off)

        return self._hash_count

    def __iter__(self):
        off = BCH_HDR_COMMON_OFF + 4  # For the version field.
        b_count, size = btcvarint_to_int(self.buf, off)
        off += size

        for i in xrange(b_count):
            yield BTCObjectHash(buf=self.buf, offset=off, length=HASH_LEN)
            off += 32

    def hash_stop(self):
        return BTCObjectHash(buf=self.buf, offset=BCH_HDR_COMMON_OFF + self.payload_len() - 32, length=HASH_LEN)


class GetHeadersBTCMessage(DataBTCMessage):
    # FIXME hashes is sharing global state
    def __init__(self, magic=None, version=None, hashes=[], hash_stop=None, buf=None):
        DataBTCMessage.__init__(self, magic, version, hashes, hash_stop, 'getheaders', buf)


class GetBlocksBTCMessage(DataBTCMessage):
    # FIXME hashes is sharing global state
    def __init__(self, magic=None, version=None, hashes=[], hash_stop=None, buf=None):
        DataBTCMessage.__init__(self, magic, version, hashes, hash_stop, 'getblocks', buf)


def pack_outpoint(hash_val, index, buf, off):
    return struct.pack_into('<32cI', buf, off, hash_val, index)


# A transaction input.
# This class cannot parse a transaction input from rawbytes, but can construct one.
class TxIn(object):
    def __init__(self, prev_outpoint_hash=None, prev_out_index=None, sig_script=None, sequence=None, buf=None, off=None,
                 length=None):
        if buf is None:
            buf = bytearray(36 + 9 + len(sig_script) + 4)
            self.buf = buf

            off = 0
            pack_outpoint(prev_outpoint_hash, prev_out_index, buf, off)
            off += 36
            off += pack_int_to_btcvarint(len(sig_script), buf, off)
            buf[off:off + len(sig_script)] = sig_script
            off += len(sig_script)
            struct.pack('<I', buf, off, sequence)
            off += 4
            self.size = off
        else:
            self.buf = buf
            self.size = length
            self.off = off

        self._memoryview = memoryview(buf)

    def rawbytes(self):
        if self.size == len(self.buf) and self.off == 0:
            return self.buf
        else:
            return self._memoryview[self.off:self.off + self.size]


# A transaction output.
# This class cannot parse a transaction output from rawbytes, but can construct one.
class TxOut(object):
    def __init__(self, value=None, pk_script=None, buf=None, off=None, length=None):
        if buf is None:
            buf = bytearray(8 + 9 + len(pk_script))
            self.buf = buf

            off = 0
            struct.pack("<Q", buf, off, value)
            off += 8
            off += pack_int_to_btcvarint(len(pk_script), buf, off)
            buf[off:off + len(pk_script)] = pk_script
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self.size = length
            self.off = off

    def rawbytes(self):
        if self.size == len(self.buf) and self.off == 0:
            return self.buf
        else:
            return self._memoryview[self.off:self.off + self.size]


# A transaction message.
# This class cannot fully parse a transaction message from the message bytes, but can construct one.
class TxBTCMessage(BTCMessage):
    # Params:
    #    - tx_in: A list of TxIn instances.
    #    - tx_out: A list of TxOut instances.

    # FIXME this constructor should call init of super class or diverge from inheritance model
    #   add magic as argument and test
    def __init__(self, version=None, tx_in=None, tx_out=None, lock_time=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 2 * 9 + 8)
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            struct.pack_into('<I', buf, off, version)
            off += 4
            off += pack_int_to_btcvarint(len(tx_in), buf, off)

            for inp in tx_in:
                rawbytes = inp.rawbytes()
                size = len(rawbytes)
                buf[off:off + size] = rawbytes
                off += size

            off += pack_int_to_btcvarint(len(tx_out), buf, off)

            for out in tx_out:
                rawbytes = out.rawbytes()
                size = len(rawbytes)
                buf[off:off + size] = rawbytes
                off += size

            struct.pack_into('<I', buf, off, lock_time)
            off += 4

            # FIXME magic is undefined
            # BTCMessage.__init__(self, magic, 'tx', off-BCH_HDR_COMMON_OFF, buf)
            raise RuntimeError('FIXME')
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = None
        self._tx_in = None
        self._tx_out = None
        self._lock_time = None
        self._tx_hash = None
        self._tx_out_count = None
        self._tx_in_count = None

    def version(self):
        if self._version is None:
            off = BCH_HDR_COMMON_OFF
            self._version = struct.unpack_from('<I', self.buf, off)
        return self._version

    def tx_in(self):
        if self._tx_in is None:
            off = BCH_HDR_COMMON_OFF + 4
            self._tx_in_count, size = btcvarint_to_int(self.buf, off)
            off += size
            self._tx_in = []

            start = off
            end = off

            for _ in xrange(self._tx_in_count):
                end += 36
                script_len, size = btcvarint_to_int(self.buf, end)
                end += size + script_len + 4
                self._tx_in.append(self.rawbytes()[start:end])
                start = end

            off = end
            self._tx_out_count, size = btcvarint_to_int(self.buf, off)
            self._tx_out = []
            off += size

            start = off
            end = off
            for _ in xrange(self._tx_out_count):
                end += 8
                script_len, size = btcvarint_to_int(self.buf, end)
                end += size + script_len
                self._tx_out.append(self.rawbytes()[start:end])

            off = end

            self._lock_time = struct.unpack_from('<I', self.buf, off)

        return self._tx_in

    def tx_out(self):
        if self._tx_in is None:
            self.tx_in()
        return self._tx_out

    def lock_time(self):
        if self._tx_in is None:
            self.tx_in()
        return self._lock_time

    def tx_hash(self):
        if self._tx_hash is None:
            self._tx_hash = BTCObjectHash(buf=sha256(sha256(self.payload()).digest()).digest(), length=HASH_LEN)
        return self._tx_hash

    def tx(self):
        return self.payload()


# Any object with an already hashed value. binary[offset:offset_length] must be the little endian representation
# of the object hash. buf[offset:offset+len] must be the big endian representation.
# The key invariant is that the last 4 bytes (binary[offset_lengh-4:offset_length]) must be uniformly randomly
# distributed.
class BTCObjectHash(object):
    def __init__(self, buf=None, offset=0, length=0, binary=None):
        if buf is not None:
            if isinstance(buf, bytearray):
                self.binary = buf[offset:offset + length]
            else:  # In case this is a memoryview.
                self.binary = bytearray(buf[offset:offset + length])
            self.binary = self.binary[::-1]
        else:
            self.binary = binary

        # This is where the big endian format will be stored
        self._buf = None
        self._hash = struct.unpack("<L", self.binary[-4:])[0]
        self._full_str = None

    def __hash__(self):
        return self._hash

    def __cmp__(self, id1):
        if id1 is None or self.binary > id1.binary:
            return -1
        elif self.binary < id1.binary:
            return 1
        return 0

    def __repr__(self):
        return repr(self.binary)

    def __getitem__(self, arg):
        return self.binary.__getitem__(arg)

    def full_string(self):
        if self._full_str is None:
            self._full_str = str(self.binary)
        return self._full_str

    def get_little_endian(self):
        return self.binary

    def get_big_endian(self):
        if self._buf is None:
            self._buf = self.binary[::-1]
        return self._buf


def pack_block_message(buf, magic, block_header, txns):
    off = BCH_HDR_COMMON_OFF
    buf[off:off + 80] = block_header
    off += 80
    num_txns = len(txns)
    off += pack_int_to_btcvarint(num_txns, buf, off)

    for i in xrange(num_txns):
        tx_buf = txns[i]
        buf[off:off + len(tx_buf)] = tx_buf
        off += len(tx_buf)

    return BTCMessage(magic, 'block', off - BCH_HDR_COMMON_OFF, buf)


class BlockBTCMessage(BTCMessage):
    def __init__(self, magic=None, version=None, prev_block=None, merkle_root=None,
                 timestamp=None, bits=None, nonce=None, txns=None, buf=None):
        if buf is None:
            total_tx_size = sum(len(tx) for tx in txns)
            buf = bytearray(BCH_HDR_COMMON_OFF + 80 + 9 + total_tx_size)
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            struct.pack_into('<I', buf, off, version)
            off += 4
            buf[off:off + 32] = prev_block.get_little_endian()
            off += 32
            buf[off:off + 32] = merkle_root.get_little_endian()
            off += 32
            struct.pack_into('<III', buf, off, timestamp, bits, nonce)
            off += 12
            off += pack_int_to_btcvarint(len(txns), buf, off)

            for tx in txns:
                buf[off:off + len(tx)] = tx
                off += len(tx)

            BTCMessage.__init__(self, magic, 'block', off - BCH_HDR_COMMON_OFF, buf)
        else:
            assert not isinstance(buf, str)
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._prev_block = self._merkle_root = self._timestamp = None
        self._bits = self._nonce = self._txn_count = self._txns = self._hash_val = None
        self._header = self._tx_offset = None

    def version(self):
        if self._version is None:
            off = BCH_HDR_COMMON_OFF
            self._version = struct.unpack_from('<I', self.buf, off)[0]
            off += 4
            self._prev_block = BTCObjectHash(self.buf, off, 32)
            off += 32
            self._merkle_root = self._memoryview[off:off + 32]
            off += 32
            self._timestamp, self._bits, self._nonce = struct.unpack_from('<III', self.buf, off)
            off += 12
            self._txn_count, size = btcvarint_to_int(self.buf, off)
            off += size
            self._tx_offset = off

        return self._version

    def prev_block(self):
        if self._version is None:
            self.version()
        return self._prev_block

    def merkle_root(self):
        if self._version is None:
            self.version()
        return self._merkle_root

    def timestamp(self):
        if self._version is None:
            self.version()
        return self._timestamp

    def bits(self):
        if self._version is None:
            self.version()
        return self._bits

    def nonce(self):
        if self._version is None:
            self.version()
        return self._nonce

    def txn_count(self):
        if self._version is None:
            self.version()
        return self._txn_count

    def header(self):
        if self._txns is None:
            if self._tx_offset is None:
                self.version()
            self._header = self._memoryview[0:self._tx_offset]
        return self._header

    def txns(self):
        if self._txns is None:
            if self._tx_offset is None:
                self.version()

            self._txns = list()

            start = self._tx_offset
            for _ in xrange(self.txn_count()):
                size = get_next_tx_size(self.buf, start)
                self._txns.append(self._memoryview[start:start + size])
                start += size

        return self._txns

    def block_hash(self):
        if self._hash_val is None:
            header = self._memoryview[BCH_HDR_COMMON_OFF:BCH_HDR_COMMON_OFF + BLOCK_HDR_SIZE - 1]
            raw_hash = sha256(sha256(header).digest()).digest()
            self._hash_val = BTCObjectHash(buf=raw_hash, length=HASH_LEN)
        return self._hash_val

    @staticmethod
    def peek_block(input_buffer):
        buf = input_buffer.peek_message(BCH_HDR_COMMON_OFF + BLOCK_HDR_SIZE - 1)
        is_here = False
        if input_buffer.length < BCH_HDR_COMMON_OFF + BLOCK_HDR_SIZE - 1:
            return is_here, None, None

        header = buf[BCH_HDR_COMMON_OFF:BCH_HDR_COMMON_OFF + BLOCK_HDR_SIZE - 1]
        raw_hash = sha256(sha256(header).digest()).digest()
        payload_len = struct.unpack_from('<L', buf, 16)[0]
        is_here = True
        return is_here, BTCObjectHash(buf=raw_hash, length=HASH_LEN), payload_len


# A BlockHeader is the first 80 bytes of the corresponding block message payload
# terminated by a null byte (\x00) to signify that there are no transactions.
class BlockHeader(object):
    def __init__(self, buf=None, version=None, prev_block=None, merkle_root=None,
                 timestamp=None, bits=None, nonce=None):
        if buf is None:
            buf = bytearray(81)
            self.buf = buf
            off = 0
            struct.pack_into('<I', buf, off, version)
            off += 4
            buf[off:off + 32] = prev_block.get_little_endian()
            off += 32
            buf[off:off + 32] = merkle_root.get_little_endian()
            off += 32
            struct.pack_into('<III', buf, off, timestamp, bits, nonce)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)

        self._version = self._prev_block = self._merkle_root = None
        self._timestamp = self._bits = self._nonce = None
        self._block_hash = None
        self._txn_count = None
        self._hash_val = None

    def version(self):
        if self._version is None:
            off = 0
            self._version = struct.unpack_from('<I', self.buf, off)[0]
            off += 4
            self._prev_block = BTCObjectHash(self.buf, off, 32)
            off += 32
            self._merkle_root = self._memoryview[off:off + 32]
            off += 32
            self._timestamp, self._bits, self._nonce = struct.unpack_from('<III', self.buf, off)
            self._txn_count, size = btcvarint_to_int(self.buf, off)
        return self._version

    def prev_block(self):
        if self._version is None:
            self.version()
        return self._prev_block

    def merkle_root(self):
        if self._version is None:
            self.version()
        return self._merkle_root

    def timestamp(self):
        if self._version is None:
            self.version()
        return self._timestamp

    def bits(self):
        if self._version is None:
            self.version()
        return self._bits

    def nonce(self):
        if self._version is None:
            self.version()
        return self._nonce

    def block_hash(self):
        if self._block_hash is None:
            header = self._memoryview[:BLOCK_HDR_SIZE - 1]  # remove the tx count at the end
            raw_hash = sha256(sha256(header).digest()).digest()
            self._hash_val = BTCObjectHash(buf=raw_hash, length=HASH_LEN)
        return self._hash_val


class HeadersBTCMessage(BTCMessage):
    def __init__(self, magic=None, headers=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 9 + len(headers) * 81)
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            off += pack_int_to_btcvarint(len(headers), buf, off)
            for header in headers:
                buf[off:off + 81] = header
                off += 81

            BTCMessage.__init__(self, magic, 'headers', off - BCH_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._headers = self._header_count = self._block_hash = None

    def hash_count(self):
        if self._header_count is None:
            raise RuntimeError('FIXME')
            # FIXME buf is undefined, change to self.buf and test
            # off = BCH_HDR_COMMON_OFF
            # self._header_count, size = btcvarint_to_int(buf, off)

        return self._header_count

    def __iter__(self):
        raise RuntimeError('FIXME')
        # FIXME buf is undefined, change to self.buf and test
        # off = BCH_HDR_COMMON_OFF
        # self._header_count, size = btcvarint_to_int(buf, off)
        # off += size
        # for _ in xrange(self._header_count):
        #     yield self._memoryview[off:off + 81]
        #     off += 81


class RejectBTCMessage(BTCMessage):
    # ccodes
    REJECT_MALFORMED = 0x01
    REJECT_INVALID = 0x10
    REJECT_OBSOLETE = 0x11
    REJECT_DUPLICATE = 0x12
    REJECT_NONSTANDARD = 0x40
    REJECT_DUST = 0x41
    REJECT_INSUFFICIENTFEE = 0x42
    REJECT_CHECKPOINT = 0x43

    def __init__(self, magic=None, message=None, ccode=None, reason=None, b_data=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF + 9 + len(message) + 1 + 9 + len(reason) + len(b_data))
            self.buf = buf

            off = BCH_HDR_COMMON_OFF
            struct.pack_into('<%dpB' % (len(message) + 1,), buf, off, message, ccode)
            off += len(message) + 1 + 1
            struct.pack_into('<%dp' % (len(reason) + 1,), buf, off, reason)
            off += len(reason) + 1
            buf[off:off + len(b_data)] = b_data
            off += len(b_data)

            BTCMessage.__init__(self, magic, 'reject', off - BCH_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._message = self._ccode = self._reason = self._data = None

    def message(self):
        if self._message is None:
            off = BCH_HDR_COMMON_OFF
            self._message = struct.unpack_from('%dp' % (len(self.buf) - off,), self.buf, off)[0]
            off += len(self._message) + 1
            self._ccode = struct.unpack_from('B', self.buf, off)[0]
            off += 1
            self._reason = struct.unpack_from('%dp' % (len(self.buf) - off,), self.buf, off)[0]
            off += len(self._reason) + 1
            self._data = self.buf[off:]
        return self._message

    def ccode(self):
        if self._message is None:
            self.message()
        return self._ccode

    def reason(self):
        if self._message is None:
            self.message()
        return self._reason

    def data(self):
        if self._message is None:
            self.message()
        return self._data


class SendHeadersBTCMessage(BTCMessage):
    def __init__(self, magic=None, buf=None):
        if buf is None:
            buf = bytearray(BCH_HDR_COMMON_OFF)
            self.buf = buf

            BTCMessage.__init__(self, magic, 'sendheaders', 0, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None


# Returns a the size of the next transaction in the buffer.
# Returns -1 if there's a parsing error (or the end goes beyond the tail).
# Input must be a bytearray object.
def get_next_tx_size(buf, off, tail=-1):
    end = off + 4
    txin_c, size = btcvarint_to_int(buf, end)
    end += size

    if end > tail > 0:
        return -1

    for _ in xrange(txin_c):
        end += 36
        script_len, size = btcvarint_to_int(buf, end)
        end += size + script_len + 4

        if end > tail > 0:
            return -1

    txout_c, size = btcvarint_to_int(buf, end)
    end += size
    for _ in xrange(txout_c):
        end += 8
        script_len, size = btcvarint_to_int(buf, end)
        end += size + script_len

        if end > tail > 0:
            return -1

    end += 4

    if end > tail > 0:
        return -1

    return end - off


message_types = {
    'version': VersionBTCMessage,
    'verack': VerAckBTCMessage,
    'ping': PingBTCMessage,
    'pong': PongBTCMessage,
    'getaddr': GetAddrBTCMessage,
    'addr': AddrBTCMessage,
    'inv': InvBTCMessage,
    'getdata': GetDataBTCMessage,
    'notfound': NotFoundBTCMessage,
    'getheaders': GetHeadersBTCMessage,
    'getblocks': GetBlocksBTCMessage,
    'tx': TxBTCMessage,
    'block': BlockBTCMessage,
    'headers': HeadersBTCMessage,
    'reject': RejectBTCMessage,
    'sendheaders': SendHeadersBTCMessage,
}
