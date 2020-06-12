from asynctest import MagicMock
from mock import call

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.eth.eth_relay_connection import EthRelayConnection
from bxgateway.feed.new_transaction_feed import NewTransactionFeed, TransactionFeedEntry
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class EthRelayConnectionTest(AbstractTestCase):
    def setUp(self) -> None:
        opts = gateway_helpers.get_gateway_opts(8000)

        self.node = MockGatewayNode(opts)
        self.node.message_converter = EthMessageConverter()
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

        expected_publication = TransactionFeedEntry(
            Sha256Hash(
                convert.hex_to_bytes(mock_eth_messages.EIP_155_TRANSACTION_HASH)
            ),
            mock_eth_messages.EIP_155_TRANSACTIONS_MESSAGE.get_transactions()[0].to_json()
        )
        self.node.feed_manager.publish_to_feed.assert_called_once_with(
                NewTransactionFeed.NAME, expected_publication
        )
