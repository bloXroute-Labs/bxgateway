import struct
from typing import Optional

from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage


class DataOntMessage(OntMessage):
    def __init__(self, magic: Optional[int] = None, length: Optional[int] = None,
                 hash_start: Optional[OntObjectHash] = None, hash_stop: Optional[OntObjectHash] = None,
                 command: Optional[bytes] = None, buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_DATA_MSG_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<B", buf, off, length)
            off += ont_constants.ONT_CHAR_LEN

            assert hash_start is not None
            assert hash_stop is not None
            buf[off:off + ont_constants.ONT_HASH_LEN] = hash_start.get_big_endian()
            off += ont_constants.ONT_HASH_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = hash_stop.get_big_endian()
            off += ont_constants.ONT_HASH_LEN

            super().__init__(magic, command, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._hash_start = self._hash_stop = None

    def hash_start(self) -> OntObjectHash:
        return OntObjectHash(
            buf=self.buf,
            offset=ont_constants.ONT_HDR_COMMON_OFF + self.payload_len() - 2 * ont_constants.ONT_HASH_LEN,
            length=ont_constants.ONT_HASH_LEN
        )

    def hash_stop(self) -> OntObjectHash:
        return OntObjectHash(
            buf=self.buf,
            offset=ont_constants.ONT_HDR_COMMON_OFF + self.payload_len() - ont_constants.ONT_HASH_LEN,
            length=ont_constants.ONT_HASH_LEN
        )
