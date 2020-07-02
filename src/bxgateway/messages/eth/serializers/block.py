from typing import List

import rlp

from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction


# pyre-fixme[13]: Attribute `header` is never initialized.
# pyre-fixme[13]: Attribute `transactions` is never initialized.
# pyre-fixme[13]: Attribute `uncles` is never initialized.
class Block(rlp.Serializable):

    fields = [
        ("header", BlockHeader),
        ("transactions", rlp.sedes.CountableList(Transaction)),
        ("uncles", rlp.sedes.CountableList(BlockHeader))
    ]

    header: BlockHeader
    transactions: List[Transaction]
    uncles: List[BlockHeader]
