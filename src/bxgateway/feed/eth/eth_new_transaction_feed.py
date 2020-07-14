from bxgateway.feed.eth.eth_transaction_feed_entry import EthTransactionFeedEntry
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.feed import Feed


class EthNewTransactionFeed(Feed[EthTransactionFeedEntry, EthRawTransaction]):
    NAME = "newTxs"
    FIELDS = ["tx_hash", "tx_contents"]

    def __init__(self) -> None:
        super().__init__(self.NAME)

    def serialize(self, raw_message: EthRawTransaction) -> EthTransactionFeedEntry:
        return EthTransactionFeedEntry(raw_message.tx_hash, raw_message.tx_contents)

