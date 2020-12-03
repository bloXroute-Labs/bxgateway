import blxr_rlp as rlp
class ShortTransaction(rlp.Serializable):
    fields = [
        ("full_transaction", rlp.sedes.big_endian_int),
        ("transaction_bytes", rlp.sedes.binary)
    ]