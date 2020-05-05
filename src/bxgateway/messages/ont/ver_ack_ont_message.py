import struct
from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class VerAckOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.VERACK

    def __init__(self, magic: Optional[int] = None, is_consensus: Optional[bool] = None,
                 buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_CHAR_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<?", buf, off, is_consensus)
            off += ont_constants.ONT_CHAR_LEN

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None
            self._is_consensus = None

    def is_consensus(self) -> bool:
        if self._is_consensus is None:
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._is_consensus, = struct.unpack_from("<?", self.buf, off)

        is_consensus = self._is_consensus
        assert isinstance(is_consensus, bool)
        return is_consensus
