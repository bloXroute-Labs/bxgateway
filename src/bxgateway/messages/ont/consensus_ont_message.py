import base64
import json
import struct
from dataclasses import dataclass
from typing import List, Optional, Union

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils import crypto, model_loader
from bxcommon.utils.blockchain_utils.ont.ont_common_utils import ont_varint_to_int, get_txid
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


@dataclass
class ConsensusMsgPayload:
    type: int
    len: int
    payload: str


class OntConsensusMessage(OntMessage, AbstractBlockMessage):
    MESSAGE_TYPE = OntMessageType.CONSENSUS

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None,
                 consensus_payload: Optional[bytes] = None, buf: Optional[Union[bytearray, memoryview]] = None):

        if buf is None:
            # pyre-fixme[6]: Expected `typing.Sized` for 1st positional only parameter to call `len`
            #  but got `Optional[bytes]`.
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + ont_constants.ONT_INT_LEN + len(consensus_payload))
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<I", buf, off, version)
            off += ont_constants.ONT_INT_LEN

            # pyre-fixme[6]: Expected `typing.Sized` for 1st positional only parameter to call `len`
            #  but got `Optional[bytes]`.
            # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for 2nd positional only parameter
            #  to call `bytearray.__setitem__` but got `Optional[bytes]`.
            buf[off:off + len(consensus_payload)] = consensus_payload
            # pyre-fixme[6]: Expected `typing.Sized` for 1st positional only parameter to call `len` but
            #  got `Optional[bytes]`.
            off += len(consensus_payload)

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._memoryview = memoryview(buf)
        self._version = self._consensus_payload = self._consensus_payload_header = self._owner_and_signature = None
        self._consensus_data = self._consensus_data_type = self._consensus_data_len = None
        self._consensus_data_full_len = self._consensus_data_str = self._consensus_data_json = None
        self._block_start_txns = self._block_start_len_memoryview = self._empty_block_offset = None
        self._prev_block = self._decoded_payload = self._payload_tail = self._message_tail = None
        self._tx_offset = self._txn_header = self._txn_count = self._hash_val = self._header_offset = None
        self._parsed = False

    def __repr__(self):
        return "ConsensusOntMessage<type: {}, length: {}, block hash: {}>".format(self.consensus_data_type(),
                                                                                  len(self.rawbytes()),
                                                                                  self._hash_val)

    def parse_message(self):
        if not self._parsed:
            self._parsed = True
        off = ont_constants.ONT_HDR_COMMON_OFF
        self._version, = struct.unpack_from("<I", self.buf, off)
        off += ont_constants.ONT_INT_LEN
        self._consensus_payload = self._memoryview[off:]
        off += ont_constants.ONT_HASH_LEN + 2 * ont_constants.ONT_INT_LEN + ont_constants.ONT_SHORT_LEN
        self._consensus_data_full_len, size = ont_varint_to_int(self.buf, off)
        off += size
        self._consensus_payload_header = self._memoryview[:off]
        self._consensus_data = self._memoryview[off:off + self._consensus_data_full_len]
        self._consensus_data_str = bytearray(self._consensus_data).decode()
        self._consensus_data_json = json.loads(self._consensus_data_str)
        off += self._consensus_data_full_len
        self._owner_and_signature = self._memoryview[off:]
        self._consensus_data_type = self._consensus_data_json["type"]
        if self._consensus_data_type == ont_constants.BLOCK_PROPOSAL_CONSENSUS_MESSAGE_TYPE:
            encoded_payload = model_loader.load_model(ConsensusMsgPayload, self._consensus_data_json)
            self._consensus_data_len = encoded_payload.len
            self._decoded_payload = memoryview(base64.b64decode(encoded_payload.payload))
            self.parse_block_vbft_type()

    def parse_block_vbft_type(self):
        block_start_len, size = ont_varint_to_int(self._decoded_payload, 0)
        self._block_start_len_memoryview = self._decoded_payload[:size]
        off = size
        block_start = self._decoded_payload[off:off + block_start_len]
        self._block_start_txns = self.parse_block_core_type(block_start)
        self._empty_block_offset = off + block_start_len
        self._payload_tail = self._decoded_payload[self._empty_block_offset:]

    def parse_block_core_type(self, buf: Union[bytearray, memoryview]) -> List[memoryview]:
        if isinstance(buf, bytearray):
            buf = memoryview(buf)
        off = ont_constants.ONT_INT_LEN
        self._prev_block = OntObjectHash(buf, off, ont_constants.ONT_HASH_LEN)
        off += ont_constants.ONT_HASH_LEN * 3 + ont_constants.ONT_BLOCK_TIME_HEIGHT_CONS_DATA_LEN
        consensus_payload_length, size = ont_varint_to_int(buf, off)
        off += consensus_payload_length + size + ont_constants.ONT_BLOCK_NEXT_BOOKKEEPER_LEN
        self._header_offset = off
        hash_header = buf[:self._header_offset]
        self._hash_val = OntObjectHash(buf=crypto.double_sha256(hash_header), length=ont_constants.ONT_HASH_LEN)
        bookkeepers_length, size = ont_varint_to_int(buf, off)
        off += size
        for _ in range(bookkeepers_length):
            _, size = ont_varint_to_int(buf, off)
            off += size + ont_constants.ONT_BOOKKEEPER_LEN

        sig_data_length, size = ont_varint_to_int(buf, off)
        off += size
        for _ in range(sig_data_length):
            sig_length, size = ont_varint_to_int(buf, off)
            off += size + sig_length

        self._txn_count, = struct.unpack_from("<L", buf, off)
        off += ont_constants.ONT_INT_LEN
        self._tx_offset = off
        self._txn_header = buf[:off]
        txns = []
        start = self._tx_offset
        txn_count = self._txn_count
        assert isinstance(txn_count, int)
        for _ in range(txn_count):
            _, off = get_txid(buf[start:])
            sig_length, size = ont_varint_to_int(buf[start:], off)
            off += size
            for i in range(sig_length):
                invoke_length, size = ont_varint_to_int(buf[start:], off)
                off += size + invoke_length
                verify_length, size = ont_varint_to_int(buf[start:], off)
                off += size + verify_length
            txns.append(buf[start:start + off])
            start += off

        return txns

    def block_start_len_memoryview(self) -> memoryview:
        if self._block_start_len_memoryview is None:
            self.parse_message()
        block_start_len_memoryview = self._block_start_len_memoryview
        assert isinstance(block_start_len_memoryview, memoryview)
        return block_start_len_memoryview

    def consensus_payload_header(self) -> memoryview:
        if self._consensus_payload_header is None:
            self.parse_message()
        consensus_payload_header = self._consensus_payload_header
        assert isinstance(consensus_payload_header, memoryview)
        return consensus_payload_header

    def consensus_data_len(self) -> int:
        if not self._parsed:
            self.parse_message()
        consensus_data_len = self._consensus_data_len
        assert isinstance(consensus_data_len, int)
        return consensus_data_len

    def consensus_data_type(self) -> int:
        if not self._parsed:
            self.parse_message()
        consensus_data_type = self._consensus_data_type
        assert isinstance(consensus_data_type, int)
        return consensus_data_type

    def txn_count(self) -> int:
        if not self._parsed:
            self.parse_message()
        txn_count = self._txn_count
        assert isinstance(txn_count, int)
        return txn_count

    def txn_header(self) -> memoryview:
        if self._txn_header is None:
            self.parse_message()
        txn_header = self._txn_header
        assert isinstance(txn_header, memoryview)
        return txn_header

    def txns(self) -> List[memoryview]:
        if not self._parsed:
            self.parse_message()
        block_start_txns = self._block_start_txns
        assert isinstance(block_start_txns, list)
        return block_start_txns

    def payload_tail(self) -> memoryview:
        if self._payload_tail is None:
            self.parse_message()
        payload_tail = self._payload_tail
        assert isinstance(payload_tail, memoryview)
        return payload_tail

    def block_hash(self) -> OntObjectHash:
        if not self._parsed:
            self.parse_message()
        hash_val = self._hash_val
        assert isinstance(hash_val, OntObjectHash)
        return hash_val

    def prev_block_hash(self) -> OntObjectHash:
        if not self._parsed:
            self.parse_message()
        prev_block = self._prev_block
        assert isinstance(prev_block, OntObjectHash), f"{prev_block} is {type(prev_block)}"
        return prev_block

    def owner_and_signature(self) -> memoryview:
        if self._owner_and_signature is None:
            self.parse_message()
        owner_and_signature = self._owner_and_signature
        assert isinstance(owner_and_signature, memoryview)
        return owner_and_signature

    def timestamp(self) -> int:
        pass

    def extra_stats_data(self):
        return ""
