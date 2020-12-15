import blxr_rlp as rlp
from bxcommon.utils.blockchain_utils.eth import eth_common_constants


class BlockHash(rlp.Serializable):
    fields = [
        ("hash", rlp.sedes.Binary.fixed_length(eth_common_constants.BLOCK_HASH_LEN)),
        ("number", rlp.sedes.big_endian_int)
    ]
