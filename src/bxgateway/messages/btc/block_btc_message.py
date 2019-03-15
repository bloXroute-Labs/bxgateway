import struct

from bxcommon.utils.log_level import LogLevel
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_BLOCK_HDR_SIZE, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import btc_varint_to_int, get_next_tx_size, pack_int_to_btc_varint
from bxcommon.utils import crypto, convert
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


# FIXME, there's a lot of duplicate code between here and BlockHeader
class BlockBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.BLOCK

    def __init__(self, magic=None, version=None, prev_block=None, merkle_root=None,
                 timestamp=None, bits=None, nonce=None, txns=None, buf=None):
        if buf is None:
            total_tx_size = sum(len(tx) for tx in txns)
            buf = bytearray(BTC_HDR_COMMON_OFF + 80 + 9 + total_tx_size)
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            struct.pack_into('<I', buf, off, version)
            off += 4
            buf[off:off + 32] = prev_block.get_little_endian()
            off += 32
            buf[off:off + 32] = merkle_root.get_little_endian()
            off += 32
            struct.pack_into('<III', buf, off, timestamp, bits, nonce)
            off += 12
            off += pack_int_to_btc_varint(len(txns), buf, off)

            for tx in txns:
                buf[off:off + len(tx)] = tx
                off += len(tx)

            super(BlockBtcMessage, self).__init__(magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            assert not isinstance(buf, str)
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._prev_block = self._merkle_root = self._timestamp = None
        self._bits = self._nonce = self._txn_count = self._txns = self._hash_val = None
        self._header = self._tx_offset = None

    def log_level(self):
        return LogLevel.INFO

    def version(self):
        if self._version is None:
            off = BTC_HDR_COMMON_OFF
            self._version = struct.unpack_from('<I', self.buf, off)[0]
            off += 4
            self._prev_block = BtcObjectHash(self.buf, off, 32)
            off += 32
            self._merkle_root = BtcObjectHash(self.buf, off, 32)
            off += 32
            self._timestamp, self._bits, self._nonce = struct.unpack_from('<III', self.buf, off)
            off += 12
            self._txn_count, size = btc_varint_to_int(self.buf, off)
            off += size
            self._tx_offset = off

            self._header = self._memoryview[0:self._tx_offset]

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
        """
        :return: seconds since epoch
        """
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
            header = self._memoryview[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE]
            raw_hash = crypto.bitcoin_hash(header)
            self._hash_val = BtcObjectHash(buf=raw_hash, length=BTC_SHA_HASH_LEN)
        return self._hash_val

    @staticmethod
    def peek_block(input_buffer):
        buf = input_buffer.peek_message(BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE)
        is_here = False
        if input_buffer.length < BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE:
            return is_here, None, None

        header = buf[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE]
        raw_hash = crypto.bitcoin_hash(header)
        payload_len = struct.unpack_from('<L', buf, 16)[0]
        is_here = True
        return is_here, BtcObjectHash(buf=raw_hash, length=BTC_SHA_HASH_LEN), payload_len

    def __repr__(self):
        return "BlockBtcMessage<block_hash: {}, length: {}>".format(self.block_hash(), len(self.rawbytes()))
