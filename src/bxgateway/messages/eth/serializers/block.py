from typing import List

import rlp

from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction


class Block(rlp.Serializable):

    fields = [
        ("header", BlockHeader),
        ("transactions", rlp.sedes.CountableList(Transaction)),
        ("uncles", rlp.sedes.CountableList(BlockHeader))
    ]

    header: BlockHeader = None
    transactions: List[Transaction] = None
    uncles: List[BlockHeader] = None
