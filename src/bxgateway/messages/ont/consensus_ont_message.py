import struct
import time
from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ont_messages_util import pack_int_to_ont_varint
from bxgateway.utils.ont.ont_object_hash import OntObjectHash


class ConsensusOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.CONSENSUS

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None,
                 prev_hash: Optional[OntObjectHash] = None, height: Optional[int] = None,
                 bookkeeper_index: Optional[int] = None, data: Optional[bytes] = None, owner: Optional[bytes] = None,
                 signature: Optional[bytes] = None, peer_id: Optional[int] = None,
                 cur_hash: Optional[OntObjectHash] = None, buf: Optional[bytearray] = None):

        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(data) + len(signature) + 119 +
                            ont_constants.ONT_VARINT_MAX_LEN * 2)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<I", buf, off, version)
            off += ont_constants.ONT_INT_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = prev_hash.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            struct.pack_into("<IHI", buf, off, height, bookkeeper_index, int(time.time()))
            off += ont_constants.ONT_CONS_HEIGHT_BKINDEX_TIME_LEN

            off += pack_int_to_ont_varint(len(data), buf, off)
            buf[off:off + len(data)] = data
            off += len(data)
            buf[off:off + ont_constants.ONT_CONS_OWNER_LEN] = owner
            off += ont_constants.ONT_CONS_OWNER_LEN
            off += pack_int_to_ont_varint(len(signature), buf, off)
            buf[off:off + len(signature)] = signature
            off += len(signature)
            struct.pack_into("<Q", buf, off, peer_id)
            off += ont_constants.ONT_LONG_LONG_LEN

            buf[off:off + ont_constants.ONT_HASH_LEN] = cur_hash.get_little_endian()
            off += ont_constants.ONT_HASH_LEN

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._memoryview = memoryview(buf)
        self._version = self._prev_hash = self._height = self._bookkeeper_index = self._timestamp = None
        self._data = self._owner = self._signature = self._peer_id = self._cur_hash = None
