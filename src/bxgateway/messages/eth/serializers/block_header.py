from typing import Dict, Any

import rlp

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.eth import eth_common_utils, eth_common_constants
from bxcommon.utils.object_hash import Sha256Hash


# pyre-fixme[13]: Attribute `number` is never initialized.
class BlockHeader(rlp.Serializable):
    FIXED_LENGTH_FIELD_OFFSET = 2 * eth_common_constants.BLOCK_HASH_LEN + eth_common_constants.ADDRESS_LEN + 3 * eth_common_constants.MERKLE_ROOT_LEN + eth_common_constants.BLOOM_LEN + 9

    fields = [
        ("prev_hash", rlp.sedes.Binary.fixed_length(eth_common_constants.BLOCK_HASH_LEN)),
        ("uncles_hash", rlp.sedes.Binary.fixed_length(eth_common_constants.BLOCK_HASH_LEN)),
        ("coinbase", rlp.sedes.Binary.fixed_length(eth_common_constants.ADDRESS_LEN, allow_empty=True)),
        ("state_root", rlp.sedes.Binary.fixed_length(eth_common_constants.MERKLE_ROOT_LEN, allow_empty=True)),
        ("tx_list_root", rlp.sedes.Binary.fixed_length(eth_common_constants.MERKLE_ROOT_LEN, allow_empty=True)),
        ("receipts_root", rlp.sedes.Binary.fixed_length(eth_common_constants.MERKLE_ROOT_LEN, allow_empty=True)),
        ("bloom", rlp.sedes.BigEndianInt(eth_common_constants.BLOOM_LEN)),
        ("difficulty", rlp.sedes.big_endian_int),
        ("number", rlp.sedes.big_endian_int),
        ("gas_limit", rlp.sedes.big_endian_int),
        ("gas_used", rlp.sedes.big_endian_int),
        ("timestamp", rlp.sedes.big_endian_int),
        ("extra_data", rlp.sedes.binary),
        ("mix_hash", rlp.sedes.binary),
        ("nonce", rlp.sedes.binary)
    ]
    
    number: int
    prev_hash: bytearray

    def __repr__(self) -> str:
        return f"EthBlockHeader<{self.hash_object()}>"

    def get_field_value(self, field_name) -> Any:
        return getattr(self, field_name, None)

    def hash(self) -> bytearray:
        """The binary block hash"""
        return eth_common_utils.keccak_hash(rlp.encode(self))

    def hash_object(self) -> Sha256Hash:
        return Sha256Hash(eth_common_utils.keccak_hash(rlp.encode(self)))

    def to_json(self) -> Dict[str, Any]:
        """
        Serializes data for publishing to the block feed.
        """
        return {
            "parent_hash": convert.bytes_to_hex_string_format(self.get_field_value('prev_hash')),
            "sha3_uncles": convert.bytes_to_hex_string_format(self.get_field_value('uncles_hash')),
            "miner": convert.bytes_to_hex_string_format(self.get_field_value("coinbase")),
            "state_root": convert.bytes_to_hex_string_format(self.get_field_value("state_root")),
            "transactions_root": convert.bytes_to_hex_string_format(self.get_field_value("tx_list_root")),
            "receipts_root": convert.bytes_to_hex_string_format(self.get_field_value("receipts_root")),
            "logs_bloom": hex(self.get_field_value("bloom")),
            "difficulty": hex(self.get_field_value("difficulty")),
            "number": hex(self.get_field_value("number")),
            "gas_limit": hex(self.get_field_value("gas_limit")),
            "gas_used": hex(self.get_field_value("gas_used")),
            "timestamp": hex(self.get_field_value("timestamp")),
            "extra_data": convert.bytes_to_hex_string_format(self.get_field_value("extra_data")),
            "mix_hash": convert.bytes_to_hex_string_format(self.get_field_value("mix_hash")),
            "nonce": convert.bytes_to_hex_string_format(self.get_field_value("nonce"))
        }
