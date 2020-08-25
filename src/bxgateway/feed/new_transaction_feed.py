from typing import NamedTuple, Dict, Any

from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.rpc import rpc_constants
from bxgateway.feed.feed import Feed
from bxgateway.feed.subscriber import Subscriber
from bxgateway.feed.feed_source import FeedSource


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
    source: FeedSource


class NewTransactionFeed(Feed[RawTransactionFeedEntry, RawTransaction]):
    NAME = rpc_constants.NEW_TRANSACTION_FEED_NAME
    FIELDS = ["tx_hash", "tx_contents"]

    def __init__(self) -> None:
        super().__init__(self.NAME)

    def subscribe(
        self, options: Dict[str, Any]
    ) -> Subscriber[RawTransactionFeedEntry]:
        include_from_blockchain = options.get("include_from_blockchain", None)
        if include_from_blockchain is not None:
            if not isinstance(include_from_blockchain, bool):
                raise RpcInvalidParams(
                    "\"include_from_blockchain\" must be a boolean"
                )
        return super().subscribe(options)

    def serialize(self, raw_message: RawTransaction) -> RawTransactionFeedEntry:
        return RawTransactionFeedEntry(raw_message.tx_hash, raw_message.tx_contents)

    def should_publish_message_to_subscriber(
        self,
        subscriber: Subscriber[RawTransactionFeedEntry],
        raw_message: RawTransaction,
        serialized_message: RawTransactionFeedEntry
    ) -> bool:
        if (
            raw_message.source == FeedSource.BLOCKCHAIN_SOCKET
            and not subscriber.options.get("include_from_blockchain", False)
        ):
            return False
        else:
            return True
