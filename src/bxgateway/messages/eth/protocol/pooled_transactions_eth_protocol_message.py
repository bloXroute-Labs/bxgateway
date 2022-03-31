from typing import List

import blxr_rlp as rlp

from bxcommon.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class PooledTransactionsEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.POOLED_TRANSACTIONS

    fields = [
        ("transactions", rlp.sedes.CountableList(Transaction))
    ]

    transactions: List[Transaction]

    def __init__(self, msg_bytes, *args, **kwargs) -> None:
        self.transactions = []
        super().__init__(msg_bytes, *args, **kwargs)

    def get_transactions(self) -> List[Transaction]:
        return self.get_field_value("transactions")
