from typing import Union, Dict, Any

from bxcommon.utils import crypto, convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.feed import Feed


class TransactionFeedEntry:
    tx_hash: str
    tx_contents: Union[str, Dict[str, Any]]

    def __init__(
        self,
        tx_hash: Sha256Hash,
        tx_contents: Union[None, bytearray, memoryview, Dict[str, Any]]
    ):
        self.tx_hash = str(tx_hash)

        if tx_contents is None:
            self.tx_contents = ""
        elif isinstance(tx_contents, (memoryview, bytearray)):
            self.tx_contents = convert.bytes_to_hex(tx_contents)
        else:
            self.tx_contents = tx_contents

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, TransactionFeedEntry)
            and other.tx_hash == self.tx_hash
            and other.tx_contents == self.tx_contents
        )


class NewTransactionFeed(Feed[TransactionFeedEntry]):
    NAME = "newTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    def __init__(self) -> None:
        super().__init__(self.NAME)
        self._active = True

    def publish(self, message: TransactionFeedEntry) -> None:
        # hex encoding is 64 bytes
        if len(message.tx_hash) != crypto.SHA256_HASH_LEN * 2:
            raise ValueError(
                f"Cannot publish a message with hash of incorrect "
                f"length: {message.tx_hash}"
            )
        super().publish(message)
