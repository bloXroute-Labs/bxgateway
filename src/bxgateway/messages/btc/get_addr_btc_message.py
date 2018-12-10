from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


class GetAddrBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.GET_ADDRESS

    def __init__(self, magic=None, buf=None):
        if buf is None:
            # We only construct empty getaddr messages.
            buf = bytearray(BTC_HDR_COMMON_OFF)
            self.buf = buf

            BtcMessage.__init__(self, magic, self.MESSAGE_TYPE, 0, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None
