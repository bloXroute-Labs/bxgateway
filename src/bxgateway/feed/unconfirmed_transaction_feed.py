from typing import Optional

from bxcommon.utils import crypto, convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.feed import Feed


class TransactionFeedEntry:
    tx_hash: str
    tx_contents: str

    def __init__(self, tx_hash: Sha256Hash, tx_contents: Optional[memoryview]):
        self.tx_hash = str(tx_hash)

        if tx_contents is None:
            self.tx_contents = ""
        else:
            self.tx_contents = convert.bytes_to_hex(tx_contents)


class UnconfirmedTransactionFeed(Feed[TransactionFeedEntry]):
    NAME = "unconfirmedTxs"
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
