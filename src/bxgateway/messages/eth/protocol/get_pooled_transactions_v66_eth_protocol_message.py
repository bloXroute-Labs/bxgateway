import blxr_rlp as rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.get_pooled_transactions_eth_protocol_message import \
    GetPooledTransactionsEthProtocolMessage


class GetPooledTransactionsV66EthProtocolMessage(GetPooledTransactionsEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(GetPooledTransactionsEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"{repr(self.get_message())}<request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> GetPooledTransactionsEthProtocolMessage:
        return GetPooledTransactionsEthProtocolMessage(None, self.transaction_hashes())
