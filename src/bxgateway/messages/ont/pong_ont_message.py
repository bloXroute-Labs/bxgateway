import struct
from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxutils.logging import LogLevel


class PongOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.PONG

    def __init__(self, magic: Optional[int] = None, height: Optional[int] = None, buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_LONG_LONG_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<Q", buf, off, height)
            off += ont_constants.ONT_LONG_LONG_LEN

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._height = None

    def height(self) -> int:
        if self._height is None:
            if len(self.buf) == ont_constants.ONT_HDR_COMMON_OFF:
                self._height = -1
            else:
                self._height, = struct.unpack_from("<Q", self.buf, ont_constants.ONT_HDR_COMMON_OFF)

        height = self._height
        assert isinstance(height, int)
        return height
