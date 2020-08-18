from asynctest import MagicMock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.connections.eth.eth_relay_connection import EthRelayConnection
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.new_transaction_feed import NewTransactionFeed, FeedSource
from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class EthRelayConnectionTest(AbstractTestCase):
    def setUp(self) -> None:
        opts = gateway_helpers.get_gateway_opts(8000)

        self.node = MockGatewayNode(opts)
        self.node.message_converter = EthNormalMessageConverter()
        self.connection = helpers.create_connection(
            EthRelayConnection,
            self.node
        )

    def test_publish_new_transaction(self):
        bx_tx_message, _, _ = self.node.message_converter.tx_to_bx_txs(
            mock_eth_messages.EIP_155_TRANSACTIONS_MESSAGE, 5
        )[0]

        self.node.feed_manager.publish_to_feed = MagicMock()
        self.connection.msg_tx(bx_tx_message)

        expected_publication = EthRawTransaction(
            bx_tx_message.tx_hash(), bx_tx_message.tx_val(), FeedSource.BDN_SOCKET
        )
        self.node.feed_manager.publish_to_feed.assert_called_once_with(
            NewTransactionFeed.NAME, expected_publication
        )

