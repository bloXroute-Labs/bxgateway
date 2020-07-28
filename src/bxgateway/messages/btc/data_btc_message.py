import struct

from bxcommon.utils.blockchain_utils.btc.btc_common_utils import btc_varint_to_int
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import pack_int_to_btc_varint


class DataBtcMessage(BtcMessage):
    def __init__(self, magic=None, version=None, hashes=None, hash_stop=None, command=None, buf=None):
        if hashes is None:
            hashes = []
        if buf is None:
            buf = bytearray(BTC_HDR_COMMON_OFF + 9 + (len(hashes) + 1) * 32)
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            struct.pack_into("<I", buf, off, version)
            off += 4
            off += pack_int_to_btc_varint(len(hashes), buf, off)

            for hash_val in hashes:
                buf[off:off + 32] = hash_val.get_big_endian()
                off += 32

            buf[off:off + 32] = hash_stop.get_big_endian()
            off += 32

            BtcMessage.__init__(self, magic, command, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._hash_count = self._hashes = self._hash_stop = None

    def version(self):
        if self._version is None:
            self._version, = struct.unpack_from("<I", self.buf, BTC_HDR_COMMON_OFF)
        return self._version

    def hash_count(self):
        if self._hash_count is None:
            off = BTC_HDR_COMMON_OFF + 4
            self._hash_count, size = btc_varint_to_int(self.buf, off)

        return self._hash_count

    def __iter__(self):
        off = BTC_HDR_COMMON_OFF + 4  # For the version field.
        b_count, size = btc_varint_to_int(self.buf, off)
        off += size

        for i in range(b_count):
            yield BtcObjectHash(buf=self.buf, offset=off, length=BTC_SHA_HASH_LEN)
            off += 32

    def hash_stop(self):
        return BtcObjectHash(buf=self.buf, offset=BTC_HDR_COMMON_OFF + self.payload_len() - 32, length=BTC_SHA_HASH_LEN)


class GetHeadersBtcMessage(DataBtcMessage):
    MESSAGE_TYPE = BtcMessageType.GET_HEADERS

    def __init__(self, magic=None, version=None, hashes=None, hash_stop=None, buf=None):
        if hashes is None:
            hashes = []
        super(GetHeadersBtcMessage, self).__init__(magic, version, hashes, hash_stop, self.MESSAGE_TYPE, buf)


class GetBlocksBtcMessage(DataBtcMessage):
    MESSAGE_TYPE = BtcMessageType.GET_BLOCKS

    def __init__(self, magic=None, version=None, hashes=None, hash_stop=None, buf=None):
        if hashes is None:
            hashes = []
        super(GetBlocksBtcMessage, self).__init__(magic, version, hashes, hash_stop, self.MESSAGE_TYPE, buf)
