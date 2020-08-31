from typing import Union, cast, List, NamedTuple, Set, Optional, TYPE_CHECKING

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.models.tx_validation_status import TxValidationStatus
from bxcommon.services.transaction_service import TransactionService, TransactionCacheKeyType, wrap_sha256, \
    TransactionFromBdnGatewayProcessingResult
from bxcommon.utils.blockchain_utils import transaction_validation
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.services.block_recovery_service import RecoveredTxsSource

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class ProcessTransactionMessageFromNodeResult(NamedTuple):
    seen: bool
    transaction_hash: Sha256Hash
    transaction_contents: Union[bytearray, memoryview]
    bdn_transaction_message: TxMessage
    tx_validation_status: TxValidationStatus


class OntTxMessage(object):
    pass


class MissingTransactions(NamedTuple):
    short_id: int
    transaction_hash: Sha256Hash


class GatewayTransactionService(TransactionService):

    node: "AbstractGatewayNode"

    def process_gateway_transaction_from_bdn(
        self,
        transaction_hash: Sha256Hash,
        short_id: int,
        transaction_contents: Union[bytearray, memoryview],
        is_compact: bool
    ) -> TransactionFromBdnGatewayProcessingResult:

        # don't check removed_contents for messages from BDN
        if (
            not short_id
            and self.has_transaction_short_id(transaction_hash)
            and self.has_transaction_contents(transaction_hash)
        ):
            return TransactionFromBdnGatewayProcessingResult(ignore_seen=True)

        existing_short_ids = self.get_short_ids(transaction_hash)
        if (self.has_transaction_contents(transaction_hash) or is_compact) \
            and short_id and short_id in existing_short_ids:
            return TransactionFromBdnGatewayProcessingResult(existing_short_id=True)

        assigned_short_id = False
        set_content = False

        if short_id and short_id not in existing_short_ids:
            self.assign_short_id(transaction_hash, short_id)
            assigned_short_id = True

        existing_contents = self.has_transaction_contents(transaction_hash)

        if not is_compact and not existing_contents:
            self.set_transaction_contents(transaction_hash, transaction_contents)
            set_content = True

        return TransactionFromBdnGatewayProcessingResult(
            assigned_short_id=assigned_short_id,
            existing_contents=existing_contents,
            set_content=set_content
        )

    def process_transactions_message_from_node(
        self,
        msg: Union[TxBtcMessage, TransactionsEthProtocolMessage, OntTxMessage],
        protocol: BlockchainProtocol,
        min_tx_network_fee: int,
        enable_transaction_validation: bool
    ) -> List[ProcessTransactionMessageFromNodeResult]:

        message_converter = cast(AbstractMessageConverter, self.node.message_converter)
        bx_tx_messages = message_converter.tx_to_bx_txs(
            msg, self.network_num, self.node.default_tx_quota_type, self.node.network.min_tx_network_fee
        )

        result = []

        for bx_tx_message, tx_hash, tx_bytes in bx_tx_messages:
            tx_cache_key = self._tx_hash_to_cache_key(tx_hash)
            tx_seen_flag = self.has_transaction_contents_by_cache_key(tx_cache_key)

            if not tx_seen_flag:
                self.set_transaction_contents(tx_hash, tx_bytes)

            tx_validation_status = TxValidationStatus.VALID_TX
            if enable_transaction_validation:
                tx_validation_status = transaction_validation.validate_transaction(
                    tx_bytes, protocol, min_tx_network_fee
                )

            result.append(
                ProcessTransactionMessageFromNodeResult(
                    tx_seen_flag, tx_hash, tx_bytes, bx_tx_message, tx_validation_status
                )
            )

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

    def set_transaction_contents_base(
        self,
        transaction_hash: Sha256Hash,
        transaction_cache_key: TransactionCacheKeyType,
        has_short_id: bool,
        previous_size: int,
        call_set_contents: bool,
        transaction_contents: Optional[Union[bytearray, memoryview]] = None,
        transaction_contents_length: Optional[int] = None
    ) -> None:
        """
        Adds transaction contents to transaction service cache with lookup key by transaction hash

        :param transaction_hash: transaction hash
        :param transaction_cache_key: transaction cache key
        :param has_short_id: flag indicating if cache already has short id for given transaction
        :param previous_size: previous size of transaction contents if already exists
        :param call_set_contents: flag indicating if method should make a call to set content form Python code
        :param transaction_contents: transaction contents bytes
        :param transaction_contents_length: if the transaction contents bytes not available, just send the length
        """
        super(GatewayTransactionService, self).set_transaction_contents_base(
            transaction_hash,
            transaction_cache_key,
            has_short_id,
            previous_size,
            call_set_contents,
            transaction_contents,
            transaction_contents_length
        )
        if transaction_contents is not None:
            self.node.log_txs_network_content(self.network_num, wrap_sha256(transaction_hash), transaction_contents)
            if call_set_contents:
                self.node.block_recovery_service.check_missing_tx_hash(
                    transaction_hash, RecoveredTxsSource.TXS_RECEIVED_FROM_NODE
                )
