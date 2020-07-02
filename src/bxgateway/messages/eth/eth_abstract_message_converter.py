
from typing import Tuple, Optional, Union, List

from bxutils import logging

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.blockchain_utils.bdn_tx_to_bx_tx import bdn_tx_to_bx_tx
from bxcommon.utils.blockchain_utils.eth.eth_common_util import raw_tx_to_bx_tx
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.abstract_message_converter import AbstractMessageConverter, BlockDecompressionResult
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.eth import rlp_utils


logger = logging.get_logger(__name__)


def parse_transaction_bytes(tx_bytes: memoryview) -> TransactionsEthProtocolMessage:
    size = len(tx_bytes)

    txs_prefix = rlp_utils.get_length_prefix_list(size)
    size += len(txs_prefix)

    buf = bytearray(size)

    buf[0:len(txs_prefix)] = txs_prefix
    buf[len(txs_prefix):] = tx_bytes
    return TransactionsEthProtocolMessage(buf)


class EthAbstractMessageConverter(AbstractMessageConverter):

    def __init__(self):
        self._last_recovery_idx: int = 0

    def tx_to_bx_txs(self, tx_msg, network_num, quota_type: Optional[QuotaType] = None) ->\
            List[Tuple[TxMessage, Sha256Hash, Union[bytearray, memoryview]]]:
        """
        Converts Ethereum transactions message to array of internal transaction messages

        The code is optimized and does not make copies of bytes

        :param tx_msg: Ethereum transaction message
        :param network_num: blockchain network number
        :param quota_type: the quota type to assign to the BDN transaction.
        :return: array of tuples (transaction message, transaction hash, transaction bytes)
        """

        if not isinstance(tx_msg, TransactionsEthProtocolMessage):
            raise TypeError("TransactionsEthProtocolMessage is expected for arg tx_msg but was {0}"
                            .format(type(tx_msg)))
        bx_tx_msgs = []

        msg_bytes = memoryview(tx_msg.rawbytes())

        _, length, start = rlp_utils.consume_length_prefix(msg_bytes, 0)
        txs_bytes = msg_bytes[start:]

        tx_start_index = 0

        while True:
            bx_tx, tx_item_length, tx_item_start = raw_tx_to_bx_tx(txs_bytes, tx_start_index, network_num, quota_type)
            bx_tx_msgs.append((bx_tx, bx_tx.message_hash(), bx_tx.tx_val()))

            tx_start_index = tx_item_start + tx_item_length

            if tx_start_index == len(txs_bytes):
                break

        return bx_tx_msgs

    def bdn_tx_to_bx_tx(
            self,
            raw_tx: Union[bytes, bytearray, memoryview],
            network_num: int,
            quota_type: Optional[QuotaType] = None
    ) -> TxMessage:
        return bdn_tx_to_bx_tx(raw_tx, network_num, quota_type)

    def bx_tx_to_tx(self, bx_tx_msg):
        """
        Converts internal transaction message to Ethereum transactions message

        The code is optimized and does not make copies of bytes

        :param bx_tx_msg: internal transaction message
        :return: Ethereum transactions message
        """

        if not isinstance(bx_tx_msg, TxMessage):
            raise TypeError("Type TxMessage is expected for bx_tx_msg arg but was {0}"
                            .format(type(bx_tx_msg)))

        return parse_transaction_bytes(bx_tx_msg.tx_val())

    def block_to_bx_block(self, block_msg: InternalEthBlockInfo, tx_service) -> Tuple[
        memoryview, BlockInfo]:
        """
        Convert Ethereum new block message to internal broadcast message with transactions replaced with short ids

        The code is optimized and does not make copies of bytes

        :param block_msg: Ethereum new block message
        :param tx_service: Transactions service
        :return: Internal broadcast message bytes (bytearray), tuple (txs count, previous block hash)
        """
        raise NotImplementedError

    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        """
        Converts internal broadcast message to Ethereum new block message

        The code is optimized and does not make copies of bytes

        :param bx_block_msg: internal broadcast message bytes
        :param tx_service: Transactions service
        :return: tuple (new block message, block hash, unknown transaction short id, unknown transaction hashes)
        """
        raise NotImplementedError
