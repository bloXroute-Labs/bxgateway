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

    # pyre-fixme[8]: Attribute has type `BlockHeader`; used as `None`.
    header: BlockHeader = None
    # pyre-fixme[8]: Attribute has type `List[Transaction]`; used as `None`.
    transactions: List[Transaction] = None
    # pyre-fixme[8]: Attribute has type `List[BlockHeader]`; used as `None`.
    uncles: List[BlockHeader] = None
