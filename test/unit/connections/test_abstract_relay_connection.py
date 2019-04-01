from mock import patch

from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256ObjectHash
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class AbstractRelayConnectionTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000,
                                                             include_default_btc_args=True,
                                                             include_default_eth_args=True))

        self.connection = AbstractRelayConnection(MockSocketConnection(), ("127.0.0.1", 12345), self.node)

    @patch("bxgateway.services.block_processing_service.BlockProcessingService._handle_decrypted_block")
    def test_msg_broadcast_encrypted(self, mock_handle_decrypted_block):
        msg_bytes = helpers.generate_bytearray(50)
        msg_hash = Sha256ObjectHash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(msg_hash=msg_hash, network_num=1, is_encrypted=True, blob=msg_bytes)

        self.connection.msg_broadcast(broadcast_msg)

        self.assertTrue(self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash))
        self.assertEqual(1, len(self.node.broadcast_messages))
        self.assertIsInstance(self.node.broadcast_messages[0][0], BlockReceivedMessage)
        mock_handle_decrypted_block.assert_not_called()

    @patch("bxgateway.services.block_processing_service.BlockProcessingService._handle_decrypted_block")
    def test_msg_broadcast_unencrypted(self, mock_handle_decrypted_block):
        msg_bytes = helpers.generate_bytearray(50)
        msg_hash = Sha256ObjectHash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(msg_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)

        self.connection.msg_broadcast(broadcast_msg)

        self.assertFalse(self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash))
        self.assertEqual(0, len(self.node.broadcast_messages))

        mock_handle_decrypted_block.assert_called_once()
