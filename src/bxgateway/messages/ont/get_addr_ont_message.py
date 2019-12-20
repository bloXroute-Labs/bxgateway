from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class GetAddrOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.GET_ADDRESS

    def __init__(self, magic: Optional[int] = None, buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF)
            self.buf = buf

            super().__init__(magic, self.MESSAGE_TYPE, 0, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None
