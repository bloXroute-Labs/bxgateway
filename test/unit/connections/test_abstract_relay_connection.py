import time
import uuid

from mock import patch, MagicMock

from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.transaction_flag import TransactionFlag
from bxgateway.testing import gateway_helpers
from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.messages.bloxroute import protocol_version
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.ping_message import PingMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.bloxroute.notification_message import NotificationMessage, NotificationCode
from bxcommon.models.entity_type_model import EntityType
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class AbstractRelayConnectionTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(
                8000,
                include_default_btc_args=True,
                include_default_eth_args=True,
                split_relays=True,
            )
        )
        self.connection = AbstractRelayConnection(
            MockSocketConnection(1, node=self.node, ip_address=constants.LOCALHOST, port=12345), self.node
        )
        self.connection.state = ConnectionState.INITIALIZED

        self.blockchain_connecton = AbstractGatewayBlockchainConnection(
            MockSocketConnection(1, node=self.node, ip_address=constants.LOCALHOST, port=333), self.node)
        self.blockchain_connecton.state = ConnectionState.ESTABLISHED

    @patch("bxgateway.services.block_processing_service.BlockProcessingService._handle_decrypted_block")
    def test_msg_broadcast_encrypted(self, mock_handle_decrypted_block):
        msg_bytes = helpers.generate_bytearray(50)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=True, blob=msg_bytes)

        self.connection.msg_broadcast(broadcast_msg)

        self.assertTrue(self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash))
        self.assertEqual(1, len(self.node.broadcast_messages))
        self.assertIsInstance(self.node.broadcast_messages[0][0], BlockReceivedMessage)
        mock_handle_decrypted_block.assert_not_called()

    @patch("bxgateway.services.block_processing_service.BlockProcessingService._handle_decrypted_block")
    def test_msg_broadcast_unencrypted(self, mock_handle_decrypted_block):
        msg_bytes = helpers.generate_bytearray(50)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)

        self.connection.msg_broadcast(broadcast_msg)

        self.assertFalse(self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash))
        self.assertEqual(0, len(self.node.broadcast_messages))

        mock_handle_decrypted_block.assert_called_once()

    def test_msg_tx__full_message(self):
        tx_service = self.connection.node.get_tx_service()
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)
        transaction_key = tx_service.get_transaction_key(tx_hash)

        self.connection.msg_tx(full_message)

        self.assertEqual(short_id, tx_service.get_short_id_by_key(transaction_key))
        self.assertEqual(tx_content, tx_service.get_transaction_by_key(transaction_key))

    def test_msg_tx__compact_then_full(self):
        tx_service = self.connection.node.get_tx_service()
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)
        transaction_key = tx_service.get_transaction_key(tx_hash)

        compact_tx_msg = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)
        no_short_id_tx_msg = TxMessage(message_hash=tx_hash, network_num=1, tx_val=tx_content)

        self.connection.msg_tx(compact_tx_msg)
        self.connection.msg_tx(no_short_id_tx_msg)

        self.assertEqual(short_id, tx_service.get_short_id_by_key(transaction_key))
        self.assertEqual(tx_content, tx_service.get_transaction_by_key(transaction_key))

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

        msg_type, payload_len = AbstractBloxrouteMessage.unpack(pong_msg_bytes[:AbstractBloxrouteMessage.HEADER_LENGTH])
        self.assertEqual(BloxrouteMessageType.PONG, msg_type)
        self.connection.advance_sent_bytes(len(pong_msg_bytes))

        time.time = MagicMock(return_value=time.time() + constants.PING_INTERVAL_S)
        self.node.alarm_queue.fire_alarms()

        ping_msg_bytes = self.connection.get_bytes_to_send()
        self.assertTrue(len(ping_msg_bytes) > 0)
        msg_type, payload_len = AbstractBloxrouteMessage.unpack(ping_msg_bytes[:AbstractBloxrouteMessage.HEADER_LENGTH])
        self.assertEqual(BloxrouteMessageType.PING, msg_type)

    @patch("bxgateway.connections.abstract_relay_connection.AbstractRelayConnection.log")
    def test_msg_notification(self, log):
        args_list = ["10", str(TransactionFlag.NO_FLAGS.value), str(EntityType.TRANSACTION.value), "100"]
        args = ",".join(args_list)
        notification_msg = NotificationMessage(NotificationCode.QUOTA_FILL_STATUS, args)
        log.assert_not_called()
        self.connection.msg_notify(notification_msg)
        log.assert_called_once()

    def test_msg_notification_assigning_short_ids(self):
        # set test connection to be transaction relay connection
        relay_id = str(uuid.uuid4())
        peer_model = OutboundPeerModel(constants.LOCALHOST, 12345, relay_id)
        self.node.peer_transaction_relays = {peer_model}
        self.connection.peer_id = relay_id
        self.connection.peer_model = peer_model

        self.assertEqual(1, len(self.node.peer_transaction_relays))
        self.connection.msg_notify(NotificationMessage(NotificationCode.ASSIGNING_SHORT_IDS))

        relay_peer = next(iter(self.node.peer_transaction_relays))
        self.assertTrue(relay_peer.assigning_short_ids)

        self.connection.msg_notify(NotificationMessage(NotificationCode.NOT_ASSIGNING_SHORT_IDS))
        relay_peer = next(iter(self.node.peer_transaction_relays))
        self.assertFalse(relay_peer.assigning_short_ids)
