from typing import Optional

from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class NotFoundOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.NOT_FOUND

    def __init__(self, magic: Optional[int] = None, block_hash: Optional[OntObjectHash] = None,
                 buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_HASH_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            assert block_hash is not None
            buf[off:off + ont_constants.ONT_HASH_LEN] = block_hash.get_little_endian()
            off += ont_constants.ONT_HASH_LEN

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._block_hash = None
