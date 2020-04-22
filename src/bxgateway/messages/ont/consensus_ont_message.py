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
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(data) + len(signature) + 119 +
                            ont_constants.ONT_VARINT_MAX_LEN * 2)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<I", buf, off, version)
            off += ont_constants.ONT_INT_LEN
            # pyre-fixme[16]: `Optional` has no attribute `get_little_endian`.
            buf[off:off + ont_constants.ONT_HASH_LEN] = prev_hash.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            struct.pack_into("<IHI", buf, off, height, bookkeeper_index, int(time.time()))
            off += ont_constants.ONT_CONS_HEIGHT_BKINDEX_TIME_LEN

            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += pack_int_to_ont_varint(len(data), buf, off)
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for 2nd
            #  param but got `Optional[bytes]`.
            buf[off:off + len(data)] = data
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += len(data)
            # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for 2nd
            #  param but got `Optional[bytes]`.
            buf[off:off + ont_constants.ONT_CONS_OWNER_LEN] = owner
            off += ont_constants.ONT_CONS_OWNER_LEN
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += pack_int_to_ont_varint(len(signature), buf, off)
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for 2nd
            #  param but got `Optional[bytes]`.
            buf[off:off + len(signature)] = signature
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
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
