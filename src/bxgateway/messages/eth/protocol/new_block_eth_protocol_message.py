import blxr_rlp as rlp
from typing import List, Optional

from bxutils.logging.log_level import LogLevel

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxcommon.messages.eth.serializers.block import Block
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxcommon.messages.eth.serializers.block_header import BlockHeader
from bxcommon.utils.blockchain_utils.eth import rlp_utils, eth_common_utils


class NewBlockEthProtocolMessage(EthProtocolMessage, AbstractBlockMessage):
    msg_type = EthProtocolMessageType.NEW_BLOCK

    fields = [("block", Block),
              ("chain_difficulty", rlp.sedes.big_endian_int)]

    # pyre-fixme[8]: Attribute has type `Block`; used as `None`.
    block: Block = None

    def __init__(self, msg_bytes, *args, **kwargs) -> None:
        super(NewBlockEthProtocolMessage, self).__init__(msg_bytes, *args, **kwargs)

        self._block_header: Optional[BlockHeader] = None
        self._block_body: Optional[Block] = None
        self._block_hash: Optional[Sha256Hash] = None
        self._timestamp: Optional[int] = None
        self._chain_difficulty: Optional[int] = None
        self._number: Optional[int] = None

    def extra_stats_data(self) -> str:
        return "Full block"

    @classmethod
    def from_new_block_parts(cls, new_block_parts: NewBlockParts,
                             total_difficulty: int) -> "NewBlockEthProtocolMessage":

        block_header = memoryview(new_block_parts.block_header_bytes)
        block_body = memoryview(new_block_parts.block_body_bytes)

        _, block_content_len, block_content_start = rlp_utils.consume_length_prefix(block_body, 0)
        block_body_conent = block_body[block_content_start:]

        content_size = len(block_header) + len(block_body_conent)

        block_item_prefix = rlp_utils.get_length_prefix_list(content_size)
        content_size += len(block_item_prefix)

        total_difficulty_item = rlp_utils.encode_int(total_difficulty)
        content_size += len(total_difficulty_item)

        new_block_item_prefix = rlp_utils.get_length_prefix_list(content_size)
        content_size += len(new_block_item_prefix)

        written_bytes = 0
        new_block_bytes = bytearray(content_size)

        new_block_bytes[written_bytes:written_bytes + len(new_block_item_prefix)] = new_block_item_prefix
        written_bytes += len(new_block_item_prefix)

        new_block_bytes[written_bytes:written_bytes + len(block_item_prefix)] = block_item_prefix
        written_bytes += len(block_item_prefix)

        new_block_bytes[written_bytes:written_bytes + len(block_header)] = block_header
        written_bytes += len(block_header)

        new_block_bytes[written_bytes:written_bytes + len(block_body_conent)] = block_body_conent
        written_bytes += len(block_body_conent)

        new_block_bytes[written_bytes:written_bytes + len(total_difficulty_item)] = total_difficulty_item
        written_bytes += len(total_difficulty_item)

        return cls(new_block_bytes)

    def log_level(self) -> LogLevel:
        return LogLevel.DEBUG

    def __repr__(self) -> str:
        return f"NewBlockEthProtocolMessage<block_hash: {self.block_hash()}, " \
               f"number: {self.number()}>"

    def get_block(self) -> Block:
        return self.get_field_value("block")

    def get_chain_difficulty(self) -> int:
        chain_difficulty = self._chain_difficulty
        if chain_difficulty is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)

            chain_difficulty, _ = rlp_utils.decode_int(block_msg_bytes, block_itm_start + block_itm_len)
            self._chain_difficulty = chain_difficulty
        return chain_difficulty

    def block_header(self) -> memoryview:
        block_header = self._block_header
        if block_header is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
            block_header = block_itm_bytes[0:block_hdr_itm_start + block_hdr_itm_len]
            self._block_header = block_header
        return block_header

    def block_hash(self) -> Sha256Hash:
        if self._block_hash is None:
            raw_hash = eth_common_utils.keccak_hash(self.block_header())
            self._block_hash = Sha256Hash(raw_hash)

        block_hash = self._block_hash
        assert block_hash is not None
        return block_hash

    def prev_block_hash(self) -> Sha256Hash:
        _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
        block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

        _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
        block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

        _, diff_itm_len, diff_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes,
                                                                          block_itm_start + block_itm_len)

        _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
        block_hdr_bytes = block_itm_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

        _, prev_block_itm_len, prev_block_itm_start = rlp_utils.consume_length_prefix(block_hdr_bytes, 0)
        prev_block_bytes = block_hdr_bytes[prev_block_itm_start:prev_block_itm_start + prev_block_itm_len]

        return Sha256Hash(prev_block_bytes)

    def timestamp(self) -> int:
        """
        :return: seconds since epoch
        """
        timestamp = self._timestamp
        if timestamp is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
            block_hdr_bytes = block_itm_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

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
        return timestamp

    def number(self) -> int:
        """
        :return: block height
        """
        number = self._number
        if number is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
            block_hdr_bytes = block_itm_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

            offset = BlockHeader.FIXED_LENGTH_FIELD_OFFSET
            _difficulty, difficulty_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += difficulty_length
            number, _ = rlp_utils.decode_int(block_hdr_bytes, offset)
            self._number = number
        return number

    def txns(self) -> List[Transaction]:
        txns = self.get_block().transactions
        assert txns is not None
        return txns
