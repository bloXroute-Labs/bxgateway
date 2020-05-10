import struct
from typing import Tuple, Optional, Union

from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class GetDataOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.GET_DATA

    def __init__(self, magic: Optional[int] = None, inv_type: Optional[Union[int, bytes]] = None,
                 block: Optional[OntObjectHash] = None, buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_GET_DATA_MSG_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            if isinstance(inv_type, int):
                struct.pack_into("<B", buf, off, inv_type)
            else:
                # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for
                #  2nd param but got `Optional[bytes]`.
                buf[off: off + ont_constants.ONT_CHAR_LEN] = inv_type
            off += ont_constants.ONT_CHAR_LEN
            assert block is not None
            buf[off:off + ont_constants.ONT_HASH_LEN] = block.get_big_endian()
            off += ont_constants.ONT_HASH_LEN

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._inv_type = None
        self._block = None

    def inv_type(self) -> Tuple[int, OntObjectHash]:
        if self._inv_type is None:
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._inv_type, = struct.unpack_from("<B", self.buf, off)
            off += ont_constants.ONT_CHAR_LEN
            self._block = OntObjectHash(buf=self.buf, offset=off, length=ont_constants.ONT_HASH_LEN)

        inv_type, block = self._inv_type, self._block
        assert isinstance(inv_type, int)
        assert isinstance(block, OntObjectHash)
        return inv_type, block
