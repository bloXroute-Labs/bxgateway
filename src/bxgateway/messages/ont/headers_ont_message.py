import struct
from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class HeadersOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.HEADERS
    
    def __init__(self, magic: Optional[int] = None, headers: Optional[list] = None, buf: Optional[bytearray] = None):
        if buf is None:
            headers_length = len(headers)
            header_length = len(headers[0])
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + headers_length * header_length +
                            ont_constants.ONT_INT_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<L", buf, off, headers_length)
            off += ont_constants.ONT_INT_LEN
            for header in headers:
                buf[off:off + len(header)] = header
                off += len(header)

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._headers = None
