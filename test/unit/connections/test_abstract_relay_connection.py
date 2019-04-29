import time

from mock import patch, MagicMock

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.messages.bloxroute import protocol_version
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.message import Message
from bxcommon.messages.bloxroute.ping_message import PingMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class AbstractRelayConnectionTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000,
                                                             include_default_btc_args=True,
                                                             include_default_eth_args=True))

        self.connection = AbstractRelayConnection(MockSocketConnection(), ("127.0.0.1", 12345), self.node)
        self.connection.state = ConnectionState.INITIALIZED

    @patch("bxgateway.services.block_processing_service.BlockProcessingService._handle_decrypted_block")
    def test_msg_broadcast_encrypted(self, mock_handle_decrypted_block):
        msg_bytes = helpers.generate_bytearray(50)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(msg_hash=msg_hash, network_num=1, is_encrypted=True, blob=msg_bytes)

        self.connection.msg_broadcast(broadcast_msg)

        self.assertTrue(self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash))
        self.assertEqual(1, len(self.node.broadcast_messages))
        self.assertIsInstance(self.node.broadcast_messages[0][0], BlockReceivedMessage)
        mock_handle_decrypted_block.assert_not_called()

    @patch("bxgateway.services.block_processing_service.BlockProcessingService._handle_decrypted_block")
    def test_msg_broadcast_unencrypted(self, mock_handle_decrypted_block):
        msg_bytes = helpers.generate_bytearray(50)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(msg_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)

        self.connection.msg_broadcast(broadcast_msg)

        self.assertFalse(self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash))
        self.assertEqual(0, len(self.node.broadcast_messages))

        mock_handle_decrypted_block.assert_called_once()

    def test_ping_pong(self):
        hello_msg = HelloMessage(protocol_version=protocol_version.PROTOCOL_VERSION, network_num=1)
        self.connection.add_received_bytes(hello_msg.rawbytes())
        self.connection.process_message()

        hello_msg_bytes = self.connection.get_bytes_to_send()
        self.assertTrue(len(hello_msg_bytes) > 0)
        self.connection.advance_sent_bytes(len(hello_msg_bytes))

        ack_msg = AckMessage()
        self.connection.add_received_bytes(ack_msg.rawbytes())
        self.connection.process_message()

        ack_msg_bytes = self.connection.get_bytes_to_send()
        self.assertTrue(len(ack_msg_bytes) > 0)
        self.connection.advance_sent_bytes(len(ack_msg_bytes))

        ping_msg = PingMessage(nonce=12345)
        self.connection.add_received_bytes(ping_msg.rawbytes())
        self.connection.process_message()

        pong_msg_bytes = self.connection.get_bytes_to_send()
        self.assertTrue(len(pong_msg_bytes) > 0)

        msg_type, payload_len = Message.unpack(pong_msg_bytes[:Message.HEADER_LENGTH])
        self.assertEqual(BloxrouteMessageType.PONG, msg_type)
        self.connection.advance_sent_bytes(len(pong_msg_bytes))

        time.time = MagicMock(return_value=time.time() + constants.PING_INTERVAL_S)
        self.node.alarm_queue.fire_alarms()

        ping_msg_bytes =  self.connection.get_bytes_to_send()
        self.assertTrue(len(ping_msg_bytes) > 0)
        msg_type, payload_len = Message.unpack(ping_msg_bytes[:Message.HEADER_LENGTH])
        self.assertEqual(BloxrouteMessageType.PING, msg_type)