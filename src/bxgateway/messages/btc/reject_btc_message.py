import struct

from bxutils.logging.log_level import LogLevel

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxgateway import btc_constants
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


class RejectBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.REJECT
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
            buf = bytearray(BTC_HDR_COMMON_OFF + 9 + len(message) + 1 + 9 + len(reason) + len(b_data))
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            struct.pack_into('<%dpB' % (len(message) + 1,), buf, off, message, ccode)
            off += len(message) + 1 + 1
            struct.pack_into('<%dp' % (len(reason) + 1,), buf, off, reason)
            off += len(reason) + 1
            buf[off:off + len(b_data)] = b_data
            off += len(b_data)

            BtcMessage.__init__(self, magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._message = self._ccode = self._reason = self._data = self._obj_hash = None

    def message(self):
        if self._message is None:
            off = BTC_HDR_COMMON_OFF
            self._message = struct.unpack_from('%dp' % (len(self.buf) - off,), self.buf, off)[0]
            off += len(self._message) + 1
            self._ccode = struct.unpack_from('B', self.buf, off)[0]
            off += 1
            self._reason = struct.unpack_from('%dp' % (len(self.buf) - off,), self.buf, off)[0]
            off += len(self._reason) + 1
            self._data = self.buf[off:]

            self._obj_hash = BtcObjectHash(buf=self.buf, offset=len(self.buf) - btc_constants.BTC_SHA_HASH_LEN,
                                           length=btc_constants.BTC_SHA_HASH_LEN)
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

    def obj_hash(self):
        if self._obj_hash is None:
            self.message()
        return self._obj_hash

    def log_level(self):
        return LogLevel.DEBUG

    def __repr__(self):
        return "RejectBtcMessage<message: {}, ccode: {}, reason: {}, obj hash {}".format(
            self.message(), self.ccode(), self.reason(), convert.bytes_to_hex(self.obj_hash().binary))
