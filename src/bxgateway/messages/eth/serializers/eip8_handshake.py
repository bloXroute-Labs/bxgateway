import blxr_rlp as rlp
from bxcommon.utils.blockchain_utils.eth import eth_common_constants


class Eip8Auth(rlp.Serializable):
    fields = [
        ("signature", rlp.sedes.Binary.fixed_length(eth_common_constants.SIGNATURE_LEN)),
        ("public_key", rlp.sedes.Binary.fixed_length(eth_common_constants.PUBLIC_KEY_LEN)),
        ("nonce", rlp.sedes.Binary.fixed_length(eth_common_constants.AUTH_NONCE_LEN)),
        ("version", rlp.sedes.big_endian_int)
    ]


class Eip8AuthAck(rlp.Serializable):
    fields = [
        ("public_key", rlp.sedes.Binary.fixed_length(eth_common_constants.PUBLIC_KEY_LEN)),
        ("nonce", rlp.sedes.Binary.fixed_length(eth_common_constants.AUTH_NONCE_LEN)),
        ("version", rlp.sedes.big_endian_int)
    ]
