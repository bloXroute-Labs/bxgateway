from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


# xversion message type in Bitcoin Cash
# implemented this type to handle the message processing error
class XversionBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.XVERSION

    def __init__(self, buf: bytearray):
        self.buf = buf
        self._memoryview = memoryview(buf)
        self._magic = self._command = self._payload_len = self._checksum = None
        self._payload = None
