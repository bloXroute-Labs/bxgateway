import rlp

from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxcommon.messages.eth.serializers.transaction import Transaction


class TransientBlockBody(rlp.Serializable):
    fields = [
        ("transactions", rlp.sedes.CountableList(Transaction)),
        ("uncles", rlp.sedes.CountableList(BlockHeader))
    ]
