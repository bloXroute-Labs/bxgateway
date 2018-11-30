import rlp

from bxgateway import eth_constants


class BlockHash(rlp.Serializable):
    fields = [
        ("hash", rlp.sedes.Binary.fixed_length(eth_constants.BLOCK_HASH_LEN)),
        ("number", rlp.sedes.big_endian_int)
    ]
