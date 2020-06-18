import rlp

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.feed.new_transaction_feed import NewTransactionFeed, TransactionFeedEntry
from bxgateway.messages.eth.serializers.transaction import Transaction


class EthRelayConnection(AbstractRelayConnection):
    def publish_new_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview
    ) -> None:
        if self.node.feed_manager.any_subscribers():
            transaction = rlp.decode(tx_contents.tobytes(), Transaction)
            self.node.feed_manager.publish_to_feed(
                NewTransactionFeed.NAME,
                TransactionFeedEntry(
                    tx_hash,
                    transaction.to_json()
                )
            )


