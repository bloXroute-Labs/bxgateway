import struct
from typing import Optional, List, Union

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils import crypto
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ont_messages_util import ont_varint_to_int, pack_int_to_ont_varint, get_txid


class BlockOntMessage(OntMessage, AbstractBlockMessage):
    MESSAGE_TYPE = OntMessageType.BLOCK

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None,
                 prev_block: Optional[OntObjectHash] = None, txns_root: Optional[OntObjectHash] = None,
                 block_root: Optional[OntObjectHash] = None, timestamp: Optional[int] = None,
                 height: Optional[int] = None, consensus_data: Optional[int] = None,
                 consensus_payload: Optional[bytes] = None, next_bookkeeper: Optional[bytes] = None,
                 bookkeepers: Optional[list] = None, sig_data: Optional[list] = None, txns: Optional[list] = None,
                 merkle_root: Optional[OntObjectHash] = None, buf: Optional[Union[bytearray, memoryview]] = None):
        if buf is None:
            # pyre-fixme[16]: `Optional` has no attribute `__iter__`.
            total_tx_size = sum(len(tx) for tx in txns)
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(consensus_payload) +
                            # pyre-fixme[6]: Expected `Sized` for 1st param but got
                            #  `Optional[List[typing.Any]]`.
                            ont_constants.ONT_BOOKKEEPER_AND_VARINT_LEN * len(bookkeepers) +
                            # pyre-fixme[6]: Expected `Sized` for 1st param but got
                            #  `Optional[List[typing.Any]]`.
                            # pyre-fixme[16]: `Optional` has no attribute `__getitem__`.
                            len(sig_data) * (len(sig_data[0]) + ont_constants.ONT_VARINT_MAX_LEN) + total_tx_size + 188
                            + ont_constants.ONT_VARINT_MAX_LEN * 3)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<I", buf, off, version)
            off += ont_constants.ONT_INT_LEN
            # pyre-fixme[16]: `Optional` has no attribute `get_little_endian`.
            buf[off:off + ont_constants.ONT_HASH_LEN] = prev_block.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = txns_root.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = block_root.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            struct.pack_into("<IIQ", buf, off, timestamp, height, consensus_data)
            off += ont_constants.ONT_BLOCK_TIME_HEIGHT_CONS_DATA_LEN

            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += pack_int_to_ont_varint(len(consensus_payload), buf, off)
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for 2nd
            #  param but got `Optional[bytes]`.
            buf[off:off + len(consensus_payload)] = consensus_payload
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += len(consensus_payload)
            struct.pack_into("<20s", buf, off, next_bookkeeper)
            off += ont_constants.ONT_BLOCK_NEXT_BOOKKEEPER_LEN

            # pyre-fixme[6]: Expected `Sized` for 1st param but got
            #  `Optional[List[typing.Any]]`.
            off += pack_int_to_ont_varint(len(bookkeepers), buf, off)
            for bk in bookkeepers:
                off += pack_int_to_ont_varint(len(bk), buf, off)
                buf[off:off + ont_constants.ONT_BOOKKEEPER_LEN] = bk
                off += ont_constants.ONT_BOOKKEEPER_LEN

            # pyre-fixme[6]: Expected `Sized` for 1st param but got
            #  `Optional[List[typing.Any]]`.
            off += pack_int_to_ont_varint(len(sig_data), buf, off)
            for sig in sig_data:
                off += pack_int_to_ont_varint(len(sig), buf, off)
                buf[off:off + len(sig)] = sig
                off += len(sig)

            # pyre-fixme[6]: Expected `Sized` for 1st param but got
            #  `Optional[List[typing.Any]]`.
            struct.pack_into("<L", buf, off, len(txns))
            off += ont_constants.ONT_INT_LEN
            for tx in txns:
                buf[off:off + len(tx)] = tx
                off += len(tx)

            buf[off:off + ont_constants.ONT_HASH_LEN] = merkle_root.get_little_endian()
            off += ont_constants.ONT_HASH_LEN

            super(BlockOntMessage, self).__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            assert not isinstance(buf, str)
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = None
        self._prev_block: Optional[OntObjectHash] = None
        self._txns_root = self._block_root = self._height = self._consensus_data = self._consensus_payload = None
        self._next_bookkeeper = self._txns = self._merkle_root = self._consensus_payload_length = None
        self._header = self._header_offset = self._header_with_program = self._header_with_program_offset = None
        self._hash_val = self._bookkeepers_length = self._sig_data_length = None
        self._txn_count = self._tx_offset = self._txn_header = None
        self._merkle_root_memoryview = None
        self._timestamp = 0
        self._parsed = False

    def __repr__(self):
        return "BlockOntMessage<block_hash: {}, length: {}>".format(self.block_hash(), len(self.rawbytes()))

    def parse_message(self) -> int:
        if not self._parsed:
            self._parsed = True
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._version, = struct.unpack_from("<I", self.buf, off)
            off += ont_constants.ONT_INT_LEN
            self._prev_block = OntObjectHash(self.buf, off, ont_constants.ONT_HASH_LEN)
            off += ont_constants.ONT_HASH_LEN
            self._txns_root = OntObjectHash(self.buf, off, ont_constants.ONT_HASH_LEN)
            off += ont_constants.ONT_HASH_LEN
            self._block_root = OntObjectHash(self.buf, off, ont_constants.ONT_HASH_LEN)
            off += ont_constants.ONT_HASH_LEN
            self._timestamp, self._height, self._consensus_data = struct.unpack_from("<IIQ", self.buf, off)
            off += ont_constants.ONT_BLOCK_TIME_HEIGHT_CONS_DATA_LEN
            self._consensus_payload_length, size = ont_varint_to_int(self.buf, off)
            # pyre-fixme[6]: Expected `int` for 1st param but got `None`.
            off += self._consensus_payload_length + size
            self._next_bookkeeper, = struct.unpack_from("<20s", self.buf, off)
            off += ont_constants.ONT_BLOCK_NEXT_BOOKKEEPER_LEN

            self._header_offset = off
            self._header = self._memoryview[0:self._header_offset]

            self._bookkeepers_length, size = ont_varint_to_int(self.buf, off)
            off += size
            # pyre-fixme[6]: Expected `int` for 1st param but got `None`.
            for _ in range(self._bookkeepers_length):
                _, size = ont_varint_to_int(self.buf, off)
                off += size + ont_constants.ONT_BOOKKEEPER_LEN

            self._sig_data_length, size = ont_varint_to_int(self.buf, off)
            off += size
            # pyre-fixme[6]: Expected `int` for 1st param but got `None`.
            for _ in range(self._sig_data_length):
                sig_length, size = ont_varint_to_int(self.buf, off)
                off += size + sig_length

            self._header_with_program_offset = off
            self._header_with_program = self._memoryview[ont_constants.ONT_HDR_COMMON_OFF:
                                                         self._header_with_program_offset]

            self._txn_count, = struct.unpack_from("<L", self.buf, off)
            off += ont_constants.ONT_INT_LEN
            self._tx_offset = off
            self._txn_header = self._memoryview[0:self._tx_offset]

        version = self._version
        assert isinstance(version, int)
        return version

    def prev_block_hash(self) -> OntObjectHash:
        if not self._parsed:
            self.parse_message()

        prev_block = self._prev_block
        assert prev_block is not None
        return prev_block

    def timestamp(self) -> int:
        """
        :return: seconds since epoch
        """
        assert self._timestamp is not None
        if not self._parsed:
            self.parse_message()
        return self._timestamp

    def txn_count(self) -> int:
        if not self._parsed:
            self.parse_message()
        txn_count = self._txn_count
        assert isinstance(txn_count, int)
        return txn_count

    def txn_offset(self) -> int:
        if not self._parsed:
            self.parse_message()
        tx_offset = self._tx_offset
        assert isinstance(tx_offset, int)
        return tx_offset

    def txn_header(self) -> memoryview:
        if not self._parsed:
            self.parse_message()
            self._txn_header = self._memoryview[0:self._tx_offset]
        txn_header = self._txn_header
        assert isinstance(txn_header, memoryview)
        return txn_header

    def txns(self) -> List[memoryview]:
        if self._txns is None:
            if self._tx_offset is None:
                self.parse_message()
            assert self._tx_offset is not None
            start = self._tx_offset
            self._txns = []
            for _ in range(self.txn_count()):
                _, off = get_txid(self._memoryview[start:])
                sig_length, size = ont_varint_to_int(self._memoryview[start:], off)
                off += size
                for _ in range(sig_length):
                    invoke_length, size = ont_varint_to_int(self._memoryview[start:], off)
                    off += size + invoke_length
                    verify_length, size = ont_varint_to_int(self._memoryview[start:], off)
                    off += size + verify_length
                # pyre-fixme[16]: Optional type has no attribute `append`.
                self._txns.append(self._memoryview[start:start + off])
                start += off
            self._merkle_root = OntObjectHash(self._memoryview[start:], 0, ont_constants.ONT_HASH_LEN)
            self._merkle_root_memoryview = self._memoryview[start:start + ont_constants.ONT_HASH_LEN]
        txns = self._txns
        assert isinstance(txns, list)
        return txns

    def block_header_offset(self) -> int:
        if not self._parsed:
            self.parse_message()
        header_offset = self._header_offset
        assert isinstance(header_offset, int)
        return header_offset

    def block_hash(self) -> OntObjectHash:
        if self._hash_val is None:
            if self._header_offset is None:
                self.parse_message()
            assert self._header_offset is not None
            header = self._memoryview[ont_constants.ONT_HDR_COMMON_OFF:self._header_offset]
            raw_hash = crypto.double_sha256(header)
            self._hash_val = OntObjectHash(buf=raw_hash, length=ont_constants.ONT_HASH_LEN)
        hash_val = self._hash_val
        assert isinstance(hash_val, OntObjectHash)
        return hash_val

    def header(self) -> memoryview:
        if self._header_with_program is None:
            self.parse_message()
        header_with_program = self._header_with_program
        assert isinstance(header_with_program, memoryview)
        return header_with_program

    def height(self) -> int:
        if self._height is None:
            self.parse_message()
        height = self._height
        assert isinstance(height, int)
        return height

    def merkle_root(self) -> memoryview:
        if self._merkle_root_memoryview is None:
            self.txns()
        merkle_root_memoryview = self._merkle_root_memoryview
        assert isinstance(merkle_root_memoryview, memoryview)
        return merkle_root_memoryview

    def version(self) -> int:
        if self._version is None:
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._version, = struct.unpack_from("<I", self.buf, off)
        version = self._version
        assert isinstance(version, int)
        return version
