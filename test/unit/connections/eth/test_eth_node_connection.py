import time

from mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils import helpers
from bxcommon.utils.stats.block_statistics_service import block_stats

from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.testing.abstract_rlpx_cipher_test import AbstractRLPxCipherTest
from bxgateway.testing.mocks import mock_eth_messages


class EthNodeConnectionTest(AbstractTestCase):
    def setUp(self) -> None:
        pub_key = "a04f30a45aae413d0ca0f219b4dcb7049857bc3f91a6351288cce603a2c9646294a02b987bf6586b370b2c22d74662355677007a14238bb037aedf41c2d08866"
        opts = helpers.get_gateway_opts(8000, include_default_eth_args=True, pub_key=pub_key,
                                 track_detailed_sent_messages=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        self.node = EthGatewayNode(opts)
        self.connection_fileno = 1
        self.connection = helpers.create_connection(EthNodeConnection, node=self.node, fileno=self.connection_fileno)

        cipher1, cipher2 = AbstractRLPxCipherTest().setup_ciphers()
        self.connection.connection_protocol.rlpx_cipher = cipher1

    def test_message_tracked_correctly_when_framed(self):
        block_message = NewBlockEthProtocolMessage(
            None,
            mock_eth_messages.get_dummy_block(1, mock_eth_messages.get_dummy_block_header(5, int(time.time()))),
            10
        )
        block_message.serialize()
        block_stats.add_block_event_by_block_hash = MagicMock()

        self.connection.enqueue_msg(block_message)
        message_length = self.connection.outputbuf.length
        print(message_length)
        for message in self.connection.message_tracker.messages:
            print(message.length)
        self.assertEqual(message_length, self.connection.message_tracker.messages[0].length + self.connection.message_tracker.messages[1].length)
        block_stats.add_block_event_by_block_hash.assert_not_called()

        self.node.on_bytes_sent(self.connection_fileno, 307)
        self.node.on_bytes_sent(self.connection_fileno, message_length - 307)
        block_stats.add_block_event_by_block_hash.assert_called_once()
