import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.transaction import Transaction


class TransactionsEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.TRANSACTIONS

    fields = [("transactions", rlp.sedes.CountableList(Transaction))]

    def __repr__(self):
        return f"TransactionsEthProtocolMessage<num_txs: {len(self.get_transactions())}>"

    def get_transactions(self):
        return self.get_field_value("transactions")
