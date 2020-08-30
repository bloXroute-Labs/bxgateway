from typing import Dict, Any

from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxgateway.feed.eth.eth_transaction_feed_entry import EthTransactionFeedEntry
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.feed import Feed
from bxgateway.feed.new_transaction_feed import FeedSource
from bxgateway.feed.subscriber import Subscriber


class EthNewTransactionFeed(Feed[EthTransactionFeedEntry, EthRawTransaction]):
    NAME = "newTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    def __init__(self) -> None:
        super().__init__(self.NAME)

    def subscribe(
        self, options: Dict[str, Any]
    ) -> Subscriber[EthTransactionFeedEntry]:
        include_from_blockchain = options.get("include_from_blockchain", None)
        if include_from_blockchain is not None:
            if not isinstance(include_from_blockchain, bool):
                raise RpcInvalidParams(
                    "\"include_from_blockchain\" must be a boolean"
                )
        return super().subscribe(options)

    def publish(self, raw_message: EthRawTransaction) -> None:
        if not self.any_subscribers_want_item(raw_message):
            return
        super().publish(raw_message)

    def serialize(self, raw_message: EthRawTransaction) -> EthTransactionFeedEntry:
        return EthTransactionFeedEntry(raw_message.tx_hash, raw_message.tx_contents)

    def any_subscribers_want_item(self, raw_message: EthRawTransaction) -> bool:
        if raw_message.source == FeedSource.BLOCKCHAIN_SOCKET:
            for subscriber in self.subscribers.values():
                if subscriber.options.get("include_from_blockchain", False):
                    return True
            return False
        return True

    def should_publish_message_to_subscriber(
        self,
        subscriber: Subscriber[EthTransactionFeedEntry],
        raw_message: EthRawTransaction,
        serialized_message: EthTransactionFeedEntry
    ) -> bool:
        if (
            raw_message.source == FeedSource.BLOCKCHAIN_SOCKET
            and not subscriber.options.get("include_from_blockchain", False)
        ):
            return False
        else:
            return True
