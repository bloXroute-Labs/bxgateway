from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.eth.eth_transaction_feed_entry import EthTransactionFeedEntry
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.feed import Feed

EXPIRATION_TIME_S = 5 * 60


class EthPendingTransactionFeed(Feed[EthTransactionFeedEntry, EthRawTransaction]):
    NAME = "pendingTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    published_transactions: ExpiringSet[Sha256Hash]

    def __init__(self, alarm_queue: AlarmQueue) -> None:
        super().__init__(self.NAME)

        # enforce uniqueness, since multiple sources can publish to
        # pending transactions (eth ws + remote)
        self.published_transactions = ExpiringSet(
            alarm_queue, EXPIRATION_TIME_S, "pendingTxs"
        )

    def publish(self, raw_message: EthRawTransaction) -> None:
        if raw_message.tx_hash in self.published_transactions:
            return

        super().publish(raw_message)

        self.published_transactions.add(raw_message.tx_hash)

    def serialize(self, raw_message: EthRawTransaction) -> EthTransactionFeedEntry:
        return EthTransactionFeedEntry(raw_message.tx_hash, raw_message.tx_contents)
