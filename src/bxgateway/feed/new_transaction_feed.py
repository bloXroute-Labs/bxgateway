from typing import NamedTuple

from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.feed import Feed


class RawTransactionFeedEntry:
    tx_hash: str
    tx_contents: str

    def __init__(
        self,
        tx_hash: Sha256Hash,
        tx_contents: memoryview
    ) -> None:
        self.tx_hash = str(tx_hash)
        self.tx_contents = convert.bytes_to_hex(tx_contents)

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, RawTransactionFeedEntry)
            and other.tx_hash == self.tx_hash
            and other.tx_contents == self.tx_contents
        )


class RawTransaction(NamedTuple):
    tx_hash: Sha256Hash
    tx_contents: memoryview


class NewTransactionFeed(Feed[RawTransactionFeedEntry, RawTransaction]):
    NAME = "newTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    def __init__(self) -> None:
        super().__init__(self.NAME)

    def serialize(self, raw_message: RawTransaction) -> RawTransactionFeedEntry:
        return RawTransactionFeedEntry(raw_message.tx_hash, raw_message.tx_contents)
