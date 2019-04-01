import rlp

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.utils.eth import crypto_utils


class Transaction(rlp.Serializable):
    fields = [
        ("nonce", rlp.sedes.big_endian_int),
        ("gas_price", rlp.sedes.big_endian_int),
        ("start_gas", rlp.sedes.big_endian_int),
        ("to", rlp.sedes.Binary.fixed_length(eth_constants.ADDRESS_LEN, allow_empty=True)),
        ("value", rlp.sedes.big_endian_int),
        ("data", rlp.sedes.binary),
        ("v", rlp.sedes.big_endian_int),
        ("r", rlp.sedes.big_endian_int),
        ("s", rlp.sedes.big_endian_int),
    ]

    def hash(self):
        """Transaction hash"""
        hash_bytes = crypto_utils.keccak_hash(rlp.encode(self))
        return Sha256Hash(hash_bytes)
