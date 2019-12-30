import struct
from typing import Optional, List

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils import crypto

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ont_messages_util import ont_varint_to_int, pack_int_to_ont_varint, get_txid
from bxgateway.utils.ont.ont_object_hash import OntObjectHash


class BlockOntMessage(OntMessage, AbstractBlockMessage):
    MESSAGE_TYPE = OntMessageType.BLOCK

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None,
                 prev_block: Optional[OntObjectHash] = None, txns_root: Optional[OntObjectHash] = None,
                 block_root: Optional[OntObjectHash] = None, timestamp: Optional[int] = None,
                 height: Optional[int] = None, consensus_data: Optional[int] = None,
                 consensus_payload: Optional[bytes] = None, next_bookkeeper: Optional[bytes] = None,
                 bookkeepers: Optional[list] = None, sig_data: Optional[list] = None, txns: Optional[list] = None,
                 merkle_root: Optional[OntObjectHash] = None, buf: Optional[bytearray] = None):
        if buf is None:
            total_tx_size = sum(len(tx) for tx in txns)
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(consensus_payload) +
                            ont_constants.ONT_BOOKKEEPER_AND_VARINT_LEN * len(bookkeepers) +
                            len(sig_data) * (len(sig_data[0]) + ont_constants.ONT_VARINT_MAX_LEN) + total_tx_size + 188
                            + ont_constants.ONT_VARINT_MAX_LEN * 3)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<I", buf, off, version)
            off += ont_constants.ONT_INT_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = prev_block.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = txns_root.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            buf[off:off + ont_constants.ONT_HASH_LEN] = block_root.get_little_endian()
            off += ont_constants.ONT_HASH_LEN
            struct.pack_into("<IIQ", buf, off, timestamp, height, consensus_data)
            off += ont_constants.ONT_BLOCK_TIME_HEIGHT_CONS_DATA_LEN

            off += pack_int_to_ont_varint(len(consensus_payload), buf, off)
            buf[off:off + len(consensus_payload)] = consensus_payload
            off += len(consensus_payload)
            struct.pack_into("<20s", buf, off, next_bookkeeper)
            off += ont_constants.ONT_BLOCK_NEXT_BOOKKEEPER_LEN

            off += pack_int_to_ont_varint(len(bookkeepers), buf, off)
            for bk in bookkeepers:
                off += pack_int_to_ont_varint(len(bk), buf, off)
                buf[off:off + ont_constants.ONT_BOOKKEEPER_LEN] = bk
                off += ont_constants.ONT_BOOKKEEPER_LEN

            off += pack_int_to_ont_varint(len(sig_data), buf, off)
            for sig in sig_data:
                off += pack_int_to_ont_varint(len(sig), buf, off)
                buf[off:off + len(sig)] = sig
                off += len(sig)

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
            off += self._consensus_payload_length + size
            self._next_bookkeeper, = struct.unpack_from("<20s", self.buf, off)
            off += ont_constants.ONT_BLOCK_NEXT_BOOKKEEPER_LEN

            self._header_offset = off
            self._header = self._memoryview[0:self._header_offset]

            self._bookkeepers_length, size = ont_varint_to_int(self.buf, off)
            off += size
            for _ in range(self._bookkeepers_length):
                _, size = ont_varint_to_int(self.buf, off)
                off += size + ont_constants.ONT_BOOKKEEPER_LEN

            self._sig_data_length, size = ont_varint_to_int(self.buf, off)
            off += size
            for _ in range(self._sig_data_length):
                sig_length, size = ont_varint_to_int(self.buf, off)
                off += size + sig_length

            self._header_with_program_offset = off

            self._txn_count, = struct.unpack_from("<L", self.buf, off)
            off += ont_constants.ONT_INT_LEN
            self._tx_offset = off
            self._txn_header = self._memoryview[0:self._tx_offset]

        return self._version

    def prev_block_hash(self) -> OntObjectHash:
        if not self._parsed:
            self.parse_message()

        assert self._prev_block is not None
        return self._prev_block

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
        assert isinstance(self._txn_count, int)
        return self._txn_count

    def txn_offset(self) -> int:
        if not self._parsed:
            self.parse_message()
        assert isinstance(self._tx_offset, int)
        return self._tx_offset

    def txn_header(self) -> memoryview:
        if not self._parsed:
            self.parse_message()
            self._txn_header = self._memoryview[0:self._tx_offset]
        assert self._txn_header is not None
        return self._txn_header

    def txns(self) -> List[memoryview]:
        if self._txns is None:
            if self._tx_offset is None:
                self.parse_message()
            assert self._tx_offset is not None
            start = self._tx_offset
            self._txns = []
            for _ in range(self.txn_count()):
                _, off = get_txid(self.buf[start:])
                sig_length, size = ont_varint_to_int(self.buf[start:], off)
                off += size
                for _ in range(sig_length):
                    invoke_length, size = ont_varint_to_int(self.buf[start:], off)
                    off += size + invoke_length
                    verify_length, size = ont_varint_to_int(self.buf[start:], off)
                    off += size + verify_length
                self._txns.append(self._memoryview[start:start + off])
                start += off
        assert isinstance(self._txns, list)
        return self._txns

    def block_header_offset(self) -> int:
        if not self._parsed:
            self.parse_message()
        assert isinstance(self._header_offset, int)
        return self._header_offset

    def block_hash(self) -> OntObjectHash:
        if self._hash_val is None:
            if self._header_offset is None:
                self.parse_message()
            assert self._header_offset is not None
            header = self._memoryview[ont_constants.ONT_HDR_COMMON_OFF:self._header_offset]
            raw_hash = crypto.double_sha256(header)
            self._hash_val = OntObjectHash(buf=raw_hash, length=ont_constants.ONT_HASH_LEN)
        return self._hash_val  # pyre-ignore

    def header(self) -> memoryview:
        if self._header_with_program_offset is None:
            self.parse_message()
            self._header_with_program = self._memoryview[ont_constants.ONT_HDR_COMMON_OFF:
                                                         self._header_with_program_offset]
        assert self._header_with_program is not None
        return self._header_with_program

    def height(self) -> int:
        if self._height is None:
            self.parse_message()

        assert self._height is not None
        return self._height
