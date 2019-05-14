import struct

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


class SendCompactBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.SEND_COMPACT

    ON_FLAG_LEN = 1
    VERSION_LEN = 8

    def __init__(self, magic=None, on_flag=None, version=None, buf=None):
        if buf is None:
            buf = bytearray(BTC_HDR_COMMON_OFF + self.ON_FLAG_LEN + self.VERSION_LEN)

            off = BTC_HDR_COMMON_OFF

            struct.pack_into("<B", buf, off, on_flag)
            off += self.ON_FLAG_LEN

            struct.pack_into("<Q", buf, off, version)
            off += self.VERSION_LEN

            BtcMessage.__init__(self, magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._memoryview = memoryview(buf)
        self._on_flag = self._version = None

    def on_flag(self):
        if self._on_flag is None:
            off = BTC_HDR_COMMON_OFF
            self._on_flag, = struct.unpack_from("<B", self.buf, off)

        return self._on_flag

    def version(self):
        if self._version is None:
            off = BTC_HDR_COMMON_OFF + self.ON_FLAG_LEN
            self._version, = struct.unpack_from("<Q", self.buf, off)

        return self._version