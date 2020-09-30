from dataclasses import dataclass
from typing import Optional

from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.messages.eth.serializers.block_header import BlockHeader
from bxcommon.utils.blockchain_utils.eth import rlp_utils, eth_common_utils


@dataclass
class NewBlockParts:
    block_header_bytes: memoryview
    block_body_bytes: memoryview
    block_number: int

    def get_block_hash(self) -> Optional[Sha256Hash]:
        if self.block_header_bytes is None:
            return None

        raw_hash = eth_common_utils.keccak_hash(self.block_header_bytes)
        return Sha256Hash(raw_hash)

    def get_previous_block_hash(self) -> Optional[Sha256Hash]:
        if self.block_header_bytes is None:
            return None

        _, header_items_len, header_items_start = rlp_utils.consume_length_prefix(self.block_header_bytes, 0)
        header_items_bytes = self.block_header_bytes[header_items_start:]

        _, prev_block_itm_len, prev_block_itm_start = rlp_utils.consume_length_prefix(header_items_bytes, 0)
        prev_block_bytes = header_items_bytes[prev_block_itm_start:prev_block_itm_start + prev_block_itm_len]

        return Sha256Hash(prev_block_bytes)

    def get_block_difficulty(self) -> Optional[int]:
        if self.block_header_bytes is None:
            return None

        _, header_items_len, header_items_start = rlp_utils.consume_length_prefix(self.block_header_bytes, 0)
        header_items_bytes = self.block_header_bytes[header_items_start:]

        block_difficulty, _ = rlp_utils.decode_int(header_items_bytes, BlockHeader.FIXED_LENGTH_FIELD_OFFSET)

        return block_difficulty
