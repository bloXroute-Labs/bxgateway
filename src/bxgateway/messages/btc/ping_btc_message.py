import random
import struct
import sys

from bxutils.logging.log_level import LogLevel

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.btc_message import BtcMessage
# FIXME dedup this against pongbtcmessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


class PingBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.PING

    def __init__(self, magic=None, buf=None):
        if buf is None:
            buf = bytearray(BTC_HDR_COMMON_OFF + 8)
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            struct.pack_into("<Q", buf, off, random.randint(0, sys.maxsize))
            off += 8

            BtcMessage.__init__(self, magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._nonce = None

    def nonce(self):
        if self._nonce is None:
            if len(self.buf) == BTC_HDR_COMMON_OFF:
                self._nonce = -1
            elif len(self.buf) == BTC_HDR_COMMON_OFF + 4:
                self._nonce = struct.unpack_from("<L", self.buf, BTC_HDR_COMMON_OFF)[0]
            else:
                self._nonce = struct.unpack_from("<Q", self.buf, BTC_HDR_COMMON_OFF)[0]

        return self._nonce

    def log_level(self) -> LogLevel:
        return LogLevel.DEBUG
