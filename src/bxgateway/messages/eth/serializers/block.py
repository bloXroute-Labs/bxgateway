from typing import List, Dict, Any

import rlp

from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxcommon.messages.eth.serializers.transaction import Transaction


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

    def to_json(self) -> Dict[str, Any]:
        """
        Serializes data for publishing to the block feed.
        """
        block_json = {
            "header": self.header.to_json(),
            "transactions": [],
            "uncles": []
        }
        transactions = []
        for transaction in self.transactions:
            transactions.append(transaction.to_json())
        block_json["transactions"] = transactions

        uncles = []
        for uncle in self.uncles:
            uncles.append(uncle.to_json())
        block_json["uncles"] = uncles

        return block_json
