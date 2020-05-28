from bxcommon.utils import crypto
from bxgateway.feed.feed import Feed
from bxgateway.feed.unconfirmed_transaction_feed import TransactionFeedEntry


class PendingTransactionFeed(Feed[TransactionFeedEntry]):
    NAME = "pendingTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    def __init__(self) -> None:
        super().__init__(self.NAME)

    def publish(self, message: TransactionFeedEntry) -> None:
        # hex encoding is 64 bytes
        if len(message.tx_hash) != crypto.SHA256_HASH_LEN * 2:
            raise ValueError(
                f"Cannot publish a message with hash of incorrect "
                f"length: {message.tx_hash}"
            )
        super().publish(message)
