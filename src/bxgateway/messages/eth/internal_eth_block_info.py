from abc import ABC
from typing import Optional, List

import blxr_rlp as rlp
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.abstract_eth_message import AbstractEthMessage
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxcommon.messages.eth.serializers.block_header import BlockHeader
from bxcommon.utils.blockchain_utils.eth import rlp_utils, eth_common_utils


class InternalEthBlockInfo(AbstractEthMessage, AbstractBlockMessage, ABC):
    """
    Internal structure that is used to transfer Ethereum new block information in BDN.
    Gateway and node may communicate new blocks using NewBlockEthProtocolMessage or NewBlockHashesEthProtocolMessage.
    This message combines information for both and used for block compression in both cases.
    The message is RLP serialized the same way as other Ethereum messages

    Message structure is: [block header] + [transactions] + [uncles] + [total difficulty] + [block number].

    Block number is 0 if the message represents NewBlockEthProtocolMessage.

    If the message represents NewBlockHashesEthProtocolMessage then block number is > 0 and total difficulty is 0.
    """

    fields = [
        ("header", BlockHeader),
        ("transactions", rlp.sedes.CountableList(Transaction)),
        ("uncles", rlp.sedes.CountableList(BlockHeader)),
        ("chain_difficulty", rlp.sedes.big_endian_int),
        ("number", rlp.sedes.big_endian_int)
    ]

    def __init__(self, msg_bytes, *args, **kwargs):
        super(InternalEthBlockInfo, self).__init__(msg_bytes, *args, **kwargs)

        self._block_header: Optional[memoryview] = None
        self._block_hash: Optional[Sha256Hash] = None
        self._timestamp: Optional[int] = None
        self._difficulty: Optional[int] = None
        self._block_number: Optional[int] = None

    def block_header(self) -> memoryview:
        if self._block_header is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            self._block_header = block_msg_bytes[0:block_hdr_itm_start + block_hdr_itm_len]

        block_header = self._block_header
        assert block_header is not None

        return block_header

    def block_hash(self) -> Sha256Hash:
        if self._block_hash is None:
            raw_hash = eth_common_utils.keccak_hash(self.block_header())
            self._block_hash = Sha256Hash(raw_hash)

        block_hash = self._block_hash
        assert block_hash is not None

        return block_hash

    def extra_stats_data(self) -> str:
        if self.has_block_number():
            if self.has_total_difficulty():
                return "New Block Hash msg; total difficulty calculated"
            else:
                return "New Block Hash msg; total difficulty not calculated"
        else:
            return "New Block msg"

    def prev_block_hash(self) -> Sha256Hash:
        _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
        block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

        _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
        block_hdr_bytes = block_msg_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

        _, prev_block_itm_len, prev_block_itm_start = rlp_utils.consume_length_prefix(block_hdr_bytes, 0)
        prev_block_bytes = block_hdr_bytes[prev_block_itm_start:prev_block_itm_start + prev_block_itm_len]

        return Sha256Hash(prev_block_bytes)

    def difficulty(self) -> int:
        """
        :return: seconds since epoch
        """
        if self._difficulty is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_hdr_bytes = block_msg_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

            offset = BlockHeader.FIXED_LENGTH_FIELD_OFFSET

            difficulty, _difficulty_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            self._difficulty = difficulty

        difficulty = self._difficulty
        assert difficulty is not None

        return difficulty

    def timestamp(self) -> int:
        """
        :return: seconds since epoch
        """
        if self._timestamp is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_hdr_bytes = block_msg_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

            offset = BlockHeader.FIXED_LENGTH_FIELD_OFFSET
            _difficulty, difficulty_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += difficulty_length
            _number, number_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += number_length
            _gas_limit, gas_limit_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += gas_limit_length
            _gas_used, gas_used_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += gas_used_length

            timestamp, _timestamp_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            self._timestamp = timestamp

        timestamp = self._timestamp
        assert timestamp is not None

        return timestamp

    def has_total_difficulty(self) -> bool:
        _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
        block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

        _, block_header_len, block_header_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
        _, txs_len, txs_start = rlp_utils.consume_length_prefix(block_msg_bytes, block_header_start + block_header_len)
        _, uncles_len, uncles_start = rlp_utils.consume_length_prefix(block_msg_bytes, txs_start + txs_len)

        chain_difficulty, _ = rlp_utils.decode_int(block_msg_bytes, uncles_start + uncles_len)

        return chain_difficulty > 0

    def has_block_number(self) -> bool:
        return self.block_number() > 0

    def block_number(self) -> int:
        if self._block_number is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_hdr_bytes = block_msg_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

            offset = BlockHeader.FIXED_LENGTH_FIELD_OFFSET
            _difficulty, difficulty_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += difficulty_length
            self._block_number, _ = rlp_utils.decode_int(block_hdr_bytes, offset)

        block_number = self._block_number
        assert block_number is not None
        return block_number

    @classmethod
    def from_new_block_msg(cls, new_block_msg: NewBlockEthProtocolMessage) -> "InternalEthBlockInfo":
        """
        Creates NewBlockInternalEthMessage from raw bytes of NewBlockEthProtocolMessage
        :param new_block_msg: new block message
        :return: NewBlockInternalEthMessage message
        """
        new_block_msg_bytes = memoryview(new_block_msg.rawbytes())

        _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(new_block_msg_bytes, 0)
        block_msg_bytes = new_block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

        msg_size = 0

        # block item already include header, transactions and uncles
        _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
        block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]
        msg_size += len(block_itm_bytes)

        difficulty_bytes = block_msg_bytes[block_msg_itm_start + block_itm_len:]
        msg_size += len(difficulty_bytes)

        block_number_bytes = rlp_utils.encode_int(new_block_msg.number())
        msg_size += len(block_number_bytes)

        msg_prefix = rlp_utils.get_length_prefix_list(
            len(block_itm_bytes) + len(difficulty_bytes) + len(block_number_bytes))
        msg_size += len(msg_prefix)

        msg_bytes = bytearray(msg_size)

        written_bytes = 0

        msg_bytes[written_bytes:written_bytes + len(msg_prefix)] = msg_prefix
        written_bytes += len(msg_prefix)

        msg_bytes[written_bytes:written_bytes + len(block_itm_bytes)] = block_itm_bytes
        written_bytes += len(block_itm_bytes)

        msg_bytes[written_bytes:written_bytes + len(difficulty_bytes)] = difficulty_bytes
        written_bytes += len(difficulty_bytes)

        msg_bytes[written_bytes:written_bytes + len(block_number_bytes)] = block_number_bytes
        written_bytes += len(block_number_bytes)

        assert written_bytes == msg_size

        return cls(msg_bytes)

    @classmethod
    def from_new_block_parts(cls, new_block_details: NewBlockParts,
                             total_difficulty: Optional[int] = 0) -> "InternalEthBlockInfo":
        """
        Creates NewBlockInternalEthMessage from block header and block body bytes

        :param new_block_details: new block details
        :param total_difficulty: total difficulty of the block if known
        :return: instance of NewBlockInternalEthMessage
        """
        block_header_bytes = new_block_details.block_header_bytes
        block_body_bytes = new_block_details.block_body_bytes
        block_number = new_block_details.block_number

        if isinstance(block_body_bytes, (bytes, bytearray)):
            block_body_bytes = memoryview(block_body_bytes)

        # block body content includes transactions and uncles
        _, block_content_len, block_content_start = rlp_utils.consume_length_prefix(block_body_bytes, 0)
        block_body_conent = block_body_bytes[block_content_start:]

        msg_size = len(block_header_bytes) + len(block_body_conent)

        total_difficulty_item = rlp_utils.encode_int(0 if total_difficulty is None else total_difficulty)
        msg_size += len(total_difficulty_item)

        block_number_bytes = rlp_utils.encode_int(block_number)
        msg_size += len(block_number_bytes)

        new_block_item_prefix = rlp_utils.get_length_prefix_list(msg_size)
        msg_size += len(new_block_item_prefix)

        written_bytes = 0
        new_block_bytes = bytearray(msg_size)

        new_block_bytes[written_bytes:written_bytes + len(new_block_item_prefix)] = new_block_item_prefix
        written_bytes += len(new_block_item_prefix)

        new_block_bytes[written_bytes:written_bytes + len(block_header_bytes)] = block_header_bytes
        written_bytes += len(block_header_bytes)

        new_block_bytes[written_bytes:written_bytes + len(block_body_conent)] = block_body_conent
        written_bytes += len(block_body_conent)

        new_block_bytes[written_bytes:written_bytes + len(total_difficulty_item)] = total_difficulty_item
        written_bytes += len(total_difficulty_item)

        new_block_bytes[written_bytes:written_bytes + len(block_number_bytes)] = block_number_bytes
        written_bytes += len(block_number_bytes)

        assert msg_size == written_bytes

        return cls(new_block_bytes)

    def to_new_block_msg(self) -> NewBlockEthProtocolMessage:
        """
        Converts message to instance of NewBlockEthProtocolMessage
        :return: instance of NewBlockEthProtocolMessage
        """

        _, msg_itm_len, msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
        msg_itm_bytes = self._memory_view[msg_itm_start:]

        offset = 0
        msg_size = 0

        _, header_len, header_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        header_bytes = msg_itm_bytes[offset:header_start + header_len]
        offset = header_start + header_len
        msg_size += len(header_bytes)

        _, txs_len, txs_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        txs_bytes = msg_itm_bytes[offset:txs_start + txs_len]
        offset = txs_start + txs_len
        msg_size += len(txs_bytes)

        _, uncles_len, uncles_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        uncles_bytes = msg_itm_bytes[offset:uncles_start + uncles_len]
        offset = uncles_start + uncles_len
        msg_size += len(uncles_bytes)

        _, total_difficulty_len, total_difficulty_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        total_difficulty_bytes = msg_itm_bytes[offset:total_difficulty_start + total_difficulty_len]
        msg_size += len(total_difficulty_bytes)

        block_prefix = rlp_utils.get_length_prefix_list(
            len(header_bytes) + len(txs_bytes) + len(uncles_bytes))
        msg_size += len(block_prefix)

        msg_prefix = rlp_utils.get_length_prefix_list(msg_size)
        msg_size += len(msg_prefix)

        result_msg_bytes = bytearray(msg_size)
        written_bytes = 0

        result_msg_bytes[written_bytes:written_bytes + len(msg_prefix)] = msg_prefix
        written_bytes += len(msg_prefix)

        result_msg_bytes[written_bytes:written_bytes + len(block_prefix)] = block_prefix
        written_bytes += len(block_prefix)

        result_msg_bytes[written_bytes:written_bytes + len(header_bytes)] = header_bytes
        written_bytes += len(header_bytes)

        result_msg_bytes[written_bytes:written_bytes + len(txs_bytes)] = txs_bytes
        written_bytes += len(txs_bytes)

        result_msg_bytes[written_bytes:written_bytes + len(uncles_bytes)] = uncles_bytes
        written_bytes += len(uncles_bytes)

        result_msg_bytes[written_bytes:written_bytes + len(total_difficulty_bytes)] = total_difficulty_bytes
        written_bytes += len(total_difficulty_bytes)

        assert written_bytes == msg_size

        return NewBlockEthProtocolMessage(result_msg_bytes)

    def to_new_block_parts(self) -> NewBlockParts:
        _, msg_itm_len, msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
        msg_itm_bytes = self._memory_view[msg_itm_start:]

        offset = 0

        _, header_len, header_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        header_bytes = msg_itm_bytes[offset:header_start + header_len]
        offset = header_start + header_len

        _, txs_len, txs_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        txs_bytes = msg_itm_bytes[offset:txs_start + txs_len]
        offset = txs_start + txs_len

        _, uncles_len, uncles_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        uncles_bytes = msg_itm_bytes[offset:uncles_start + uncles_len]
        offset = uncles_start + uncles_len

        _, total_difficulty_len, total_difficulty_start = rlp_utils.consume_length_prefix(msg_itm_bytes, offset)
        offset = total_difficulty_start + total_difficulty_len

        block_number, _ = rlp_utils.decode_int(msg_itm_bytes, offset)

        block_body_prefix = rlp_utils.get_length_prefix_list(len(txs_bytes) + len(uncles_bytes))
        block_body_bytes = bytearray(len(block_body_prefix) + len(txs_bytes) + len(uncles_bytes))

        block_body_bytes[:len(block_body_prefix)] = block_body_prefix
        written_bytes = len(block_body_prefix)

        block_body_bytes[written_bytes:written_bytes + len(txs_bytes)] = txs_bytes
        written_bytes += len(txs_bytes)

        block_body_bytes[written_bytes:written_bytes + len(uncles_bytes)] = uncles_bytes
        written_bytes += len(uncles_bytes)

        return NewBlockParts(header_bytes, block_body_bytes, block_number)

    @classmethod
    def unpack(cls, buf):
        """
        Unpack buffer into command, metadata (e.g. magic number, checksum), and payload.
        """
        raise NotImplementedError()

    @classmethod
    def validate_payload(cls, buf, unpacked_args) -> None:
        """
        Validates unpacked content.
        """
        raise NotImplementedError()

    def txns(self) -> List[Transaction]:
        return self.get_field_value("transactions")
