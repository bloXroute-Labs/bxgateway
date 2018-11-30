import rlp

from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction


class Block(rlp.Serializable):

    fields = [
        ("header", BlockHeader),
        ("transactions", rlp.sedes.CountableList(Transaction)),
        ("uncles", rlp.sedes.CountableList(BlockHeader))
    ]

    header = None
    transactions = None
    uncles = None
