import struct
from typing import Optional
from bxgateway import btc_constants
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


class FeeFilterBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.FEE_FILTER

    def __init__(self, magic: Optional[int] = None, buf: Optional[bytearray] = None, fee_rate: Optional[int] = None):
        if buf is None:
            buf = bytearray(btc_constants.BTC_HDR_COMMON_OFF + 8)
            self.buf = buf

            off = btc_constants.BTC_HDR_COMMON_OFF
            struct.pack_into("<Q", buf, off, fee_rate)
            off += 8

            super().__init__(magic, self.MESSAGE_TYPE, off - btc_constants.BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._fee_rate = None

    def fee_rate(self) -> int:
        if self._fee_rate is None:
            self._fee_rate = struct.unpack_from("<Q", self.buf, btc_constants.BTC_HDR_COMMON_OFF)[0]

        # pyre-fixme[7]: Expected `int` but got `None`.
        return self._fee_rate
