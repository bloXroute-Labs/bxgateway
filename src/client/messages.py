# Copyright (C) 2017, Cornell University, All rights reserved.
# See the file COPYING for details.
#
# Different types of messages for the bloXroute wire protocol.
#
import hashlib
import struct

from blx_exceptions import *
from utils import *

sha256 = hashlib.sha256

##
# MESSAGE CLASSES
##

# The length of everything in the header minus the checksum
HDR_COMMON_OFF = 16
# Length of a sha256 hash
HASH_LEN = 32


class Message:
    def __init__(self, msg_type=None, payload_len=None, buf=None):
        self.buf = buf
        self._memoryview = memoryview(buf)

        off = 0
        struct.pack_into('<12sL', buf, off, msg_type, payload_len)
        off += 16

        self._msg_type = msg_type
        self._payload_len = payload_len
        self._payload = None

    # Returns a memoryview of the message.
    def rawbytes(self):
        if self._payload_len is None:
            self._payload_len, _ = struct.unpack_from('<L', self.buf, 12)

        if self._payload_len + HDR_COMMON_OFF == len(self.buf):
            return self._memoryview
        else:
            return self._memoryview[0:self._payload_len + HDR_COMMON_OFF]

    # peek at message, return (is_a_full_message, msg_type, length)
    # input_buffer is an instance of InputBuffer
    @staticmethod
    def peek_message(input_buffer):
        buf = input_buffer.peek_message(HDR_COMMON_OFF)
        if len(buf) < HDR_COMMON_OFF:
            return False, None, None

        _msg_type, _payload_len = struct.unpack_from('<12sL', buf, 0)
        _msg_type = _msg_type.rstrip('\x00')
        if _payload_len <= input_buffer.length - HDR_COMMON_OFF:
            return True, _msg_type, _payload_len
        return False, _msg_type, _payload_len

    # parse a full message
    @staticmethod
    def parse(buf):
        if len(buf) < HDR_COMMON_OFF:
            return None

        _msg_type, _payload_len = struct.unpack_from('<12sL', buf, 0)
        _msg_type = _msg_type.rstrip('\x00')

        if _payload_len != len(buf) - HDR_COMMON_OFF:
            log_err("Payload length does not match buffer size! Payload is %d. Buffer is %d bytes long" % (
                _payload_len, len(buf)))
            raise PayloadLenError(
                "Payload length does not match buffer size! Payload is %d. Buffer is %d bytes long" % (
                    _payload_len, len(buf)))

        if _msg_type not in message_types:
            raise UnrecognizedCommandError("%s message not recognized!" % (_msg_type,), "Raw data: %s" % (repr(buf),))

        cls = message_types[_msg_type]
        msg = cls(buf=buf)
        msg._msg_type, msg._payload_len = _msg_type, _payload_len
        return msg

    def msg_type(self):
        return self._msg_type

    def payload_len(self):
        if self._payload_len is None:
            self._payload_len = struct.unpack_from('<L', self.buf, 12)[0]
        return self._payload_len

    def checksum(self):
        # FIXME _checksum does not exist

        raise AttributeError("FIXME")
        # return self._checksum

    def payload(self):
        if self._payload is None:
            self._payload = self.buf[HDR_COMMON_OFF:self.payload_len() + HDR_COMMON_OFF]
        return self._payload


class HelloMessage(Message):
    # idx is the index of the peer. Clients will use 0 for the index and will not be connected back to.
    # Other bloXroute servers will send a positive identity specified in the config file
    def __init__(self, idx=None, buf=None):

        if buf is None:
            buf = bytearray(HDR_COMMON_OFF + 4)
            self.buf = buf
            struct.pack_into('<L', buf, HDR_COMMON_OFF, idx)

            Message.__init__(self, 'hello', 4, buf)
        else:
            self.buf = buf
            self._msg_type = self._payload_len = self._payload = None

        self._idx = None
        self._memoryview = memoryview(buf)

    def idx(self):
        if self._idx is None:
            off = HDR_COMMON_OFF
            self._idx, = struct.unpack_from('<L', self.buf, off)
        return self._idx


class PingMessage(Message):
    def __init__(self, nonce=None, buf=None):
        if buf is None:
            buf = bytearray(HDR_COMMON_OFF + 8)
            off = HDR_COMMON_OFF
            struct.pack_into('<Q', buf, off, nonce)
            off += 8

            Message.__init__(self, 'ping', off - HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._nonce = self._command = self._payload_len = None

    def nonce(self):
        if self._nonce is None:
            off = HDR_COMMON_OFF
            self._nonce, = struct.unpack_from('<Q', self.buf, off)
        return self._nonce


# XXX: Duplicated from Ping
class PongMessage(Message):
    def __init__(self, nonce=None, buf=None):
        if buf is None:
            buf = bytearray(HDR_COMMON_OFF + 8)
            off = HDR_COMMON_OFF
            struct.pack_into('<Q', buf, off, nonce)
            off += 8

            Message.__init__(self, 'pong', off - HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._nonce = self._command = self._payload_len = None

    def nonce(self):
        if self._nonce is None:
            off = HDR_COMMON_OFF
            self._nonce, = struct.unpack_from('<Q', self.buf, off)
        return self._nonce


class AckMessage(Message):
    def __init__(self, buf=None):
        if buf is None:
            buf = bytearray(HDR_COMMON_OFF)
            self.buf = buf

            Message.__init__(self, 'ack', 0, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._command = self._payload_len = None
            self._payload = None


class BlobMessage(Message):
    def __init__(self, command=None, msg_hash=None, blob=None, buf=None):
        if buf is None:
            buf = bytearray(HDR_COMMON_OFF + len(blob))
            self.buf = buf

            off = HDR_COMMON_OFF
            self.buf[off:off + 32] = msg_hash.binary
            off += 32
            self.buf[off:off + len(blob)] = blob
            off += len(blob)

            Message.__init__(self, command, off - HDR_COMMON_OFF, buf)
        else:
            assert type(buf) != "str"
            self.buf = buf
            self._memoryview = memoryview(self.buf)

        self._blob = self._msg_hash = None

    def msg_hash(self):
        if self._msg_hash is None:
            off = HDR_COMMON_OFF
            self._msg_hash = ObjectHash(self._memoryview[off:off + 32])
        return self._msg_hash

    def blob(self):
        if self._blob is None:
            off = HDR_COMMON_OFF + 32
            self._blob = self._memoryview[off:off + self.payload_len()]

        return self._blob

    @staticmethod
    def peek_message(inputbuf):
        buf = inputbuf.peek_message(HDR_COMMON_OFF + 32)
        if len(buf) < HDR_COMMON_OFF + 32:
            return False, None, None
        _, length = struct.unpack_from('<12sL', buf, 0)
        msg_hash = ObjectHash(buf[HDR_COMMON_OFF:HDR_COMMON_OFF + 32])
        return True, msg_hash, length


class BroadcastMessage(BlobMessage):
    def __init__(self, msg_hash=None, blob=None, buf=None):
        BlobMessage.__init__(self, 'broadcast', msg_hash, blob, buf)


class TxMessage(BlobMessage):
    def __init__(self, msg_hash=None, blob=None, buf=None):
        BlobMessage.__init__(self, 'tx', msg_hash, blob, buf)


# Assign a transaction an id
class TxAssignMessage(Message):
    def __init__(self, tx_hash=None, short_id=None, buf=None):
        if buf is None:
            # 32 for tx_hash, 4 for short_id
            buf = bytearray(HDR_COMMON_OFF + 36)
            off = HDR_COMMON_OFF
            buf[off:off + 32] = tx_hash.binary
            off += 32
            struct.pack_into('<L', buf, off, short_id)
            off += 4

            Message.__init__(self, 'txassign', off - HDR_COMMON_OFF, buf)
        else:
            assert type(buf) != 'str'
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._tx_hash = self._short_id = None

    def tx_hash(self):
        if self._tx_hash is None:
            off = HDR_COMMON_OFF
            self._tx_hash = ObjectHash(self.buf[off:off + 32])
        return self._tx_hash

    def short_id(self):
        if self._short_id is None:
            self._short_id, = struct.unpack_from('<L', self.buf, HDR_COMMON_OFF + 32)
        return self._short_id


class ObjectHash:
    # binary is a memoryview or a bytearray
    def __init__(self, binary):
        assert len(binary) == 32

        self.binary = binary
        if type(self.binary) == memoryview:
            self.binary = bytearray(binary)

        self._hash = struct.unpack("<L", self.binary[-4:])[0]

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


message_types = {'hello': HelloMessage,
                 'ack': AckMessage,
                 'ping': PingMessage,
                 'pong': PongMessage,
                 'broadcast': BroadcastMessage,
                 'tx': TxMessage,
                 'txassign': TxAssignMessage,
                 }
