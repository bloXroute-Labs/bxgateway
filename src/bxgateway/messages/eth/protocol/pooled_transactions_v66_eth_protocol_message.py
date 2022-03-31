import blxr_rlp as rlp

from bxcommon.utils.blockchain_utils.eth import rlp_utils
from bxgateway.messages.eth.protocol.pooled_transactions_eth_protocol_message import \
    PooledTransactionsEthProtocolMessage


class PooledTransactionsV66EthProtocolMessage(PooledTransactionsEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(PooledTransactionsEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"<PooledTransactionsEthProtocolMessage {len(self.get_transactions())} request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> PooledTransactionsEthProtocolMessage:
        _, tx_msg_itm_len, tx_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
        _, request_id_len, request_id_start = rlp_utils.consume_length_prefix(self._memory_view, tx_msg_itm_start)
        pooled_tx_msg_bytes = self._memory_view[request_id_len+request_id_start:]
        return PooledTransactionsEthProtocolMessage(pooled_tx_msg_bytes)
