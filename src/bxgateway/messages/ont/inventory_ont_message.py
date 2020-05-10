import struct
from enum import Enum
from typing import Tuple, List, Optional, Union

from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class InventoryOntType(Enum):
    MSG_TX = 1
    MSG_BLOCK = 2
    MSG_CONSENSUS = 224


class InvOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.INVENTORY

    def __init__(self, magic: Optional[int] = None, inv_type: Optional[Union[InventoryOntType, bytes]] = None,
                 blocks: Optional[List[OntObjectHash]] = None, buf: Optional[bytearray] = None):
        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_CHAR_LEN + ont_constants.ONT_INT_LEN +
                            # pyre-fixme[6]: Expected `Sized` for 1st param but got
                            #  `Optional[List[OntObjectHash]]`.
                            ont_constants.ONT_HASH_LEN * len(blocks))
            self.buf = buf
            # pyre-fixme[6]: Expected `Sized` for 1st param but got
            #  `Optional[List[OntObjectHash]]`.
            self.blocks_len = len(blocks)

            off = ont_constants.ONT_HDR_COMMON_OFF
            if isinstance(inv_type, InventoryOntType):
                struct.pack_into("<B", buf, off, inv_type.value)
            else:
                # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for
                #  2nd param but got `Optional[bytes]`.
                buf[off: off + ont_constants.ONT_CHAR_LEN] = inv_type
            off += ont_constants.ONT_CHAR_LEN
            # pyre-fixme[6]: Expected `Sized` for 1st param but got
            #  `Optional[List[OntObjectHash]]`.
            struct.pack_into("<L", buf, off, len(blocks))
            off += ont_constants.ONT_INT_LEN

            # pyre-fixme[16]: `Optional` has no attribute `__iter__`.
            for block in blocks:
                buf[off:off + ont_constants.ONT_HASH_LEN] = block.get_big_endian()
                off += ont_constants.ONT_HASH_LEN

            super(InvOntMessage, self).__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self.blocks_len = 0 if blocks is None else len(blocks)
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._inv_type: Optional[int] = None
        self._block_hashes: Optional[List[OntObjectHash]] = None
        self._len_blocks: Optional[int] = None

    def __repr__(self):
        if self._inv_type is None:
            self.inv_type()
        return "InventoryOntMessage<length:{}, inventory type: {}, number of blocks:{}, blocks: {}>".format(
            len(self.rawbytes()), self._inv_type, self._len_blocks, self._block_hashes)

    def inv_type(self) -> Tuple[int, List[OntObjectHash]]:
        if self._inv_type is None:
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._inv_type, = struct.unpack_from("<B", self.buf, off)
            off += ont_constants.ONT_CHAR_LEN
            self._len_blocks, = struct.unpack_from("<L", self.buf, off)
            off += ont_constants.ONT_INT_LEN
            self._block_hashes = []
            len_blocks = self._len_blocks
            assert isinstance(len_blocks, int)
            for _ in range(len_blocks):
                # pyre-fixme[16]: `Optional` has no attribute `append`.
                self._block_hashes.append(OntObjectHash(buf=self.buf, offset=off, length=ont_constants.ONT_HASH_LEN))
                off += ont_constants.ONT_HASH_LEN

        inv_type, block_hashes = self._inv_type, self._block_hashes
        assert isinstance(inv_type, int)
        assert isinstance(block_hashes, list)
        return inv_type, block_hashes
