from bxcommon.utils import crypto
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway.feed.feed import Feed
from bxgateway.feed.unconfirmed_transaction_feed import TransactionFeedEntry

EXPIRATION_TIME_S = 5 * 60


class PendingTransactionFeed(Feed[TransactionFeedEntry]):
    NAME = "pendingTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    published_transactions: ExpiringSet[str]

    def __init__(self, alarm_queue: AlarmQueue) -> None:
        super().__init__(self.NAME)

        # enforce uniqueness, since multiple sources can publish to
        # pending transactions (eth ws + remote)
        self.published_transactions = ExpiringSet(
            alarm_queue, EXPIRATION_TIME_S, "pendingTxs"
        )

    def publish(self, message: TransactionFeedEntry) -> None:
        # hex encoding is 64 bytes
        if len(message.tx_hash) != crypto.SHA256_HASH_LEN * 2:
            raise ValueError(
                f"Cannot publish a message with hash of incorrect "
                f"length: {message.tx_hash}"
            )

        if message.tx_hash in self.published_transactions:
            return

        super().publish(message)

        self.published_transactions.add(message.tx_hash)
