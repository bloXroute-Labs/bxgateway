from typing import List

import blxr_rlp as rlp
from bxcommon.messages.eth.serializers.block_header import BlockHeader
from bxcommon.messages.eth.serializers.transaction import Transaction

# pyre-fixme[13]: Attribute `transactions` is never initialized.
# pyre-fixme[13]: Attribute `uncles` is never initialized.
class TransientBlockBody(rlp.Serializable):
    fields = [
        ("transactions", rlp.sedes.CountableList(Transaction)),
        ("uncles", rlp.sedes.CountableList(BlockHeader))
    ]

    transactions: List[Transaction]
    uncles: List[BlockHeader]
