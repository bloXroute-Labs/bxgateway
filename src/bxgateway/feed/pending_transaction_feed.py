import time

from bxcommon.utils import crypto
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway.feed.feed import Feed
from bxgateway.feed.new_transaction_feed import TransactionFeedEntry
from bxgateway import gateway_constants

EXPIRATION_TIME_S = 5 * 60


class PendingTransactionFeed(Feed[TransactionFeedEntry]):
    NAME = "pendingTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    published_transactions: ExpiringSet[str]
    last_published_event_ts: float

    def __init__(self, alarm_queue: AlarmQueue) -> None:
        super().__init__(self.NAME)

        # enforce uniqueness, since multiple sources can publish to
        # pending transactions (eth ws + remote)
        self.published_transactions = ExpiringSet(
            alarm_queue, EXPIRATION_TIME_S, "pendingTxs"
        )
        self.last_published_event_ts = 0

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
        self.last_published_event_ts = time.time()
        self.published_transactions.add(message.tx_hash)

    @property
    def active(self) -> bool:
        return (self.last_published_event_ts +
                gateway_constants.PENDING_TX_FEED_CONSIDER_INACTIVE_INTERVAL_S > time.time())

    def keep_alive(self) -> None:
        self.last_published_event_ts = time.time()
