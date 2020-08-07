from typing import Union, cast, List, NamedTuple, Set

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage


class ProcessTransactionMessageFromNodeResult(NamedTuple):
    seen: bool
    transaction_hash: Sha256Hash
    transaction_contents: Union[bytearray, memoryview]
    bdn_transaction_message: TxMessage


class OntTxMessage(object):
    pass


class MissingTransactions(NamedTuple):
    short_id: int
    transaction_hash: Sha256Hash


class GatewayTransactionService(TransactionService):

    def process_transactions_message_from_node(
        self,
        msg: Union[TxBtcMessage, TransactionsEthProtocolMessage, OntTxMessage]
    ) -> List[ProcessTransactionMessageFromNodeResult]:

        # pyre-fixme[16]: `bxcommon.connections.abstract_node.AbstractNode` has no attribute `message_converter`.
        message_converter = cast(AbstractMessageConverter, self.node.message_converter)

        # pyre-fixme[16]: `bxcommon.connections.abstract_node.AbstractNode` has no attribute `default_tx_quota_type`.
        bx_tx_messages = message_converter.tx_to_bx_txs(msg, self.network_num, self.node.default_tx_quota_type)

        result = []

        for bx_tx_message, tx_hash, tx_bytes in bx_tx_messages:

            tx_cache_key = self._tx_hash_to_cache_key(tx_hash)

            tx_seen_flag = self.has_transaction_contents_by_cache_key(tx_cache_key) or \
                           self.removed_transaction_by_cache_key(tx_cache_key)

            if not tx_seen_flag:
                self.set_transaction_contents(tx_hash, tx_bytes)

            result.append(ProcessTransactionMessageFromNodeResult(
                tx_seen_flag,
                tx_hash,
                tx_bytes,
                bx_tx_message
            ))

        return result

    def process_txs_message(
        self,
        msg: TxsMessage
    ) -> Set[MissingTransactions]:
        missing_transactions: Set[MissingTransactions] = set()
        transactions = msg.get_txs()
        missing: bool
        for transaction in transactions:
            transaction_hash, transaction_contents, short_id = transaction
            missing = False

            assert transaction_hash is not None
            assert short_id is not None

            if not self.has_short_id(short_id):
                self.assign_short_id(transaction_hash, short_id)
                missing = True

            if not self.has_transaction_contents(transaction_hash):
                assert transaction_contents is not None
                self.set_transaction_contents(transaction_hash, transaction_contents)

                missing = True

            if missing:
                missing_transactions.add(MissingTransactions(short_id, transaction_hash))

        return missing_transactions