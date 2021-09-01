import blxr_rlp as rlp

from bxgateway.messages.eth.protocol.pooled_transactions_eth_protocol_message import \
    PooledTransactionsEthProtocolMessage


class PooledTransactionsV66EthProtocolMessage(PooledTransactionsEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(PooledTransactionsEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"{repr(self.get_message())}<request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> PooledTransactionsEthProtocolMessage:
        return PooledTransactionsEthProtocolMessage(None, self.get_transactions())
