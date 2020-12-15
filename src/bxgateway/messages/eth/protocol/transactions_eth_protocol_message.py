from typing import List

import blxr_rlp as rlp
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxcommon.messages.eth.serializers.transaction import Transaction


class TransactionsEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.TRANSACTIONS

    fields = [("transactions", rlp.sedes.CountableList(Transaction))]

    def __repr__(self):
        # Calling get_transactions here causes transactions message to be deserialized in Python code and impacts
        # performance. Print only length of the message instead.
        return f"TransactionsEthProtocolMessage<message_len: {len(self.rawbytes())}>"

    def get_transactions(self) -> List[Transaction]:
        return self.get_field_value("transactions")
