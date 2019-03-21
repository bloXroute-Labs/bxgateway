import time
from unittest import skip

from mock import MagicMock

from bxcommon.connections.connection_pool import ConnectionPool
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import LOCALHOST
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxgateway import gateway_constants
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.data_btc_message import GetHeadersBtcMessage
from bxgateway.messages.btc.headers_btc_message import HeadersBtcMessage
from bxgateway.messages.gateway.blockchain_sync_request_message import BlockchainSyncRequestMessage
from bxgateway.messages.gateway.blockchain_sync_response_message import BlockchainSyncResponseMessage
from bxgateway.services.blockchain_sync_service import BlockchainSyncService
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


@skip("Service is not currently used.")
class BlockchainPassThroughServiceTest(AbstractTestCase):
    MAGIC = 12345
    VERSION = 70014
    HASH1 = BtcObjectHash(binary=crypto.double_sha256(b"hash1"))
    HASH2 = BtcObjectHash(binary=crypto.double_sha256(b"hash2"))
    HASHES = [HASH1, HASH2]

    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000))
        self.node.connection_pool = ConnectionPool()
        self.blockchain_sync_service = BlockchainSyncService(self.node, {
            BtcMessageType.GET_HEADERS: BtcMessageType.HEADERS
        })
        self.blockchain_connection = MockConnection(1, (LOCALHOST, 8001), self.node)
        self.blockchain_connection.state |= ConnectionState.ESTABLISHED
        self.node.node_conn = self.blockchain_connection
        self.alarms = []

    def test_make_sync_request(self):
        other_connections = []
        for i in range(2, 5):
            fileno = i
            port = 8000 + i
            connection = MockConnection(fileno, (LOCALHOST, port), self.node)
            connection.state |= ConnectionState.ESTABLISHED
            connection.enqueue_msg = MagicMock()
            self.node.connection_pool.add(fileno, LOCALHOST, port, connection)
            other_connections.append(connection)

        preferred_connection = other_connections[0]
        self.node.peer_gateways.add(OutboundPeerModel(LOCALHOST, 8002))

        request = GetHeadersBtcMessage(self.MAGIC, self.VERSION, self.HASHES, self.HASH2)
        self.blockchain_sync_service.make_sync_request(request.command(), request)

        request_message = BlockchainSyncRequestMessage(request.command(), request.rawbytes())
        preferred_connection.enqueue_msg.assert_called_once_with(request_message, prepend=True)

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCKCHAIN_SYNC_BROADCAST_DELAY_S)
        self.node.alarm_queue.fire_alarms()
        self.assertIn(request_message, self.node.broadcast_messages)

    def test_remote_sync_response_preferred_response(self):
        self.node.send_msg_to_node = MagicMock()
        responding_connection = GatewayConnection(MockSocketConnection(), (LOCALHOST, 8002), self.node)

        request = GetHeadersBtcMessage(self.MAGIC, self.VERSION, self.HASHES, self.HASH2)
        self.blockchain_sync_service.make_sync_request(request.command(), request)

        remote_response = HeadersBtcMessage(self.MAGIC, headers=[])
        self.blockchain_sync_service.process_remote_sync_response(remote_response.command(), remote_response,
                                                                  responding_connection)

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCKCHAIN_SYNC_BROADCAST_DELAY_S)
        self.node.alarm_queue.fire_alarms()

        self.node.send_msg_to_node.assert_called_once_with(remote_response)
        self.assertEqual(0, len(self.blockchain_sync_service.awaiting_local_callback_connections))
        self.assertEqual(0, len(self.node.broadcast_messages))

    def test_remote_sync_response_broadcast_response(self):
        self.node.send_msg_to_node = MagicMock()
        not_responding_connection = GatewayConnection(MockSocketConnection(1), (LOCALHOST, 8002), self.node)
        not_responding_connection.enqueue_msg = MagicMock()
        not_responding_connection.state |= ConnectionState.ESTABLISHED
        responding_connection = GatewayConnection(MockSocketConnection(2), (LOCALHOST, 8003), self.node)
        responding_connection.enqueue_msg = MagicMock()
        responding_connection.state |= ConnectionState.ESTABLISHED

        self.node.connection_pool.add(1, LOCALHOST, 8002, not_responding_connection)
        self.node.connection_pool.add(2, LOCALHOST, 8003, responding_connection)
        self.node.peer_gateways.add(OutboundPeerModel(LOCALHOST, 8002, is_internal=True))
        self.node.peer_gateways.add(OutboundPeerModel(LOCALHOST, 8003, is_internal=False))

        request = GetHeadersBtcMessage(self.MAGIC, self.VERSION, self.HASHES, self.HASH2)
        self.blockchain_sync_service.make_sync_request(request.command(), request)

        not_responding_connection.enqueue_msg.assert_called_once()
        responding_connection.enqueue_msg.assert_not_called()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCKCHAIN_SYNC_BROADCAST_DELAY_S)
        self.node.alarm_queue.fire_alarms()

        self.assertEqual(1, len(self.node.broadcast_messages))

        remote_response = HeadersBtcMessage(self.MAGIC, headers=[])
        self.blockchain_sync_service.process_remote_sync_response(remote_response.command(), remote_response,
                                                                  responding_connection)

        self.node.send_msg_to_node.assert_called_once_with(remote_response)
        self.assertEqual(0, len(self.blockchain_sync_service.awaiting_local_callback_connections))
        self.assertEqual(responding_connection, self.node.get_preferred_gateway_connection())

    def test_remote_sync_request(self):
        self.node.send_msg_to_node = MagicMock()
        remote_caller_connection1 = MockConnection(1, (LOCALHOST, 8001), self.node)
        remote_caller_connection1.enqueue_msg = MagicMock()
        remote_caller_connection2 = MockConnection(2, (LOCALHOST, 8002), self.node)
        remote_caller_connection2.enqueue_msg = MagicMock()

        remote_blockchain_message = GetHeadersBtcMessage(self.MAGIC, self.VERSION, self.HASHES, self.HASH2)
        self.blockchain_sync_service.process_remote_sync_request(remote_blockchain_message.command(),
                                                                 remote_blockchain_message, remote_caller_connection1)
        self.blockchain_sync_service.process_remote_sync_request(remote_blockchain_message.command(),
                                                                 remote_blockchain_message, remote_caller_connection2)
        self.node.send_msg_to_node.assert_called_once_with(remote_blockchain_message)

        local_blockchain_message = HeadersBtcMessage(self.MAGIC, headers=[])
        self.blockchain_sync_service.process_local_sync_response(local_blockchain_message.command(),
                                                                 local_blockchain_message)

        local_response = BlockchainSyncResponseMessage(local_blockchain_message.command(),
                                                       local_blockchain_message.rawbytes())

        remote_caller_connection1.enqueue_msg.assert_called_once_with(local_response)

