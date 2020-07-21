from bxcommon.utils.blockchain_utils.eth import rlp_utils

# pylint: disable=invalid-name
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage


def parse_transaction_bytes(tx_bytes: memoryview) -> TransactionsEthProtocolMessage:
    size = len(tx_bytes)

    txs_prefix = rlp_utils.get_length_prefix_list(size)
    size += len(txs_prefix)

    buf = bytearray(size)

    buf[0:len(txs_prefix)] = txs_prefix
    buf[len(txs_prefix):] = tx_bytes
    return TransactionsEthProtocolMessage(buf)
