from typing import TYPE_CHECKING

from bxcommon.utils.blockchain_utils.eth import eth_common_utils
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.new_transaction_feed import FeedSource

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode


class EthRelayConnection(AbstractRelayConnection):
    node: "EthGatewayNode"

    def publish_new_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview
    ) -> None:
        gas_price = eth_common_utils.raw_tx_gas_price(tx_contents, 0)
        if gas_price >= self.node.get_network_min_transaction_fee():
            self.node.feed_manager.publish_to_feed(
                EthNewTransactionFeed.NAME,
                EthRawTransaction(tx_hash, tx_contents, FeedSource.BDN_SOCKET)
            )
