from typing import Dict, Any

from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxcommon.rpc import rpc_constants
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.eth.eth_transaction_feed_entry import EthTransactionFeedEntry
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.eth import eth_filter_handlers
from bxgateway.feed.feed import Feed
from bxgateway.feed.subscriber import Subscriber
from bxgateway.feed import filter_dsl
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

logger = logging.get_logger()
logger_filters = logging.get_logger(LogRecordType.TransactionFiltering, __name__)

EXPIRATION_TIME_S = 5 * 60


class EthPendingTransactionFeed(Feed[EthTransactionFeedEntry, EthRawTransaction]):
    NAME = rpc_constants.ETH_PENDING_TRANSACTION_FEED_NAME
    FIELDS = ["tx_hash", "tx_contents"]
    FILTERS = {"transaction_value_range_eth", "from", "to"}

    published_transactions: ExpiringSet[Sha256Hash]

    def __init__(self, alarm_queue: AlarmQueue) -> None:
        super().__init__(self.NAME)

        # enforce uniqueness, since multiple sources can publish to
        # pending transactions (eth ws + remote)
        self.published_transactions = ExpiringSet(
            alarm_queue, EXPIRATION_TIME_S, "pendingTxs"
        )

    def subscribe(self, options: Dict[str, Any]) -> Subscriber[EthTransactionFeedEntry]:
        duplicates = options.get("duplicates", None)
        if duplicates is not None:
            if not isinstance(duplicates, bool):
                raise RpcInvalidParams('"duplicates" must be a boolean')

        return super().subscribe(options)

    def publish(self, raw_message: EthRawTransaction) -> None:
        if (
            raw_message.tx_hash in self.published_transactions
            and not self.any_subscribers_want_duplicates()
        ):
            return

        super().publish(raw_message)

        self.published_transactions.add(raw_message.tx_hash)

    def serialize(self, raw_message: EthRawTransaction) -> EthTransactionFeedEntry:
        return EthTransactionFeedEntry(raw_message.tx_hash, raw_message.tx_contents)

    def any_subscribers_want_duplicates(self) -> bool:
        for subscriber in self.subscribers.values():
            if subscriber.options.get("duplicates", False):
                return True
        return False

    def should_publish_message_to_subscriber(
        self,
        subscriber: Subscriber[EthTransactionFeedEntry],
        raw_message: EthRawTransaction,
        serialized_message: EthTransactionFeedEntry,
    ) -> bool:
        if (
            raw_message.tx_hash in self.published_transactions
            and not subscriber.options.get("duplicates", False)
        ):
            return False
        should_publish = True
        if subscriber.filters:
            logger_filters.trace(
                "checking if should publish to {} with filters {}",
                subscriber.subscription_id,
                subscriber.filters,
            )
            should_publish = filter_dsl.handle(
                subscriber.filters,
                eth_filter_handlers.handle_filter,
                serialized_message,
            )
        logger_filters.trace("should publish: {}", should_publish)
        return should_publish

    def reformat_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        return filter_dsl.reformat(filters, eth_filter_handlers.reformat_filter)
