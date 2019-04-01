import socket

from mock import MagicMock

from bxcommon.connections.connection_pool import ConnectionPool
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import LOCALHOST, DEFAULT_NETWORK_NUM
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_node import MockOpts
from bxcommon.utils import crypto
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.gateway_connection import GatewayConnection
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxgateway.messages.gateway.block_propagation_request import BlockPropagationRequestMessage
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_version_manager import gateway_version_manager
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.neutrality_service import NeutralityService

from bxgateway.services.blockchain_sync_service import BlockchainSyncService


def create_node(ip=LOCALHOST, port=8000):
    node = MagicMock(spec=AbstractGatewayNode)
    node.opts = MockOpts(external_ip=ip, external_port=port)
    node.connection_pool = ConnectionPool()
    node.connection_exists = node.connection_pool.has_connection
    node.network_num = node.opts.blockchain_network_num
    node.in_progress_blocks = BlockEncryptedCache(AlarmQueue())
    node.neutrality_service = MagicMock(spec=NeutralityService)
    node.blockchain_sync_service = MagicMock(spec=BlockchainSyncService)
    node.block_processing_service = MagicMock(spec=BlockProcessingService)
    node.alarm_queue = None
    return node


class GatewayConnectionTest(AbstractTestCase):
    def setUp(self):
        self.node = create_node(LOCALHOST, 8000)
        self.connection = self._create_connection(1, LOCALHOST, 8000, True)

    def _create_connection(self, fileno=1, ip=LOCALHOST, port=8001, from_me=True, node=None):
        if node is None:
            node = self.node
        sock = MagicMock(spec=socket.socket)
        sock.fileno = lambda: fileno
        connection = GatewayConnection(SocketConnection(sock, node), (ip, port), node, from_me)

        node.connection_pool.add(fileno, ip, port, connection)
        return connection

    def test_msg_hello_only_connection(self):
        main_node = create_node(LOCALHOST, 40001)
        peer_node = create_node(LOCALHOST, 40000)
        main_inbound_fileno = 1
        main_inbound_connection = self._create_connection(main_inbound_fileno, LOCALHOST, 50000, False, main_node)
        peer_outbound_fileno = 2
        peer_outbound_connection = self._create_connection(peer_outbound_fileno, LOCALHOST, 40001, True, peer_node)

        self.assertTrue(main_node.connection_pool.has_connection(LOCALHOST, 50000))

        main_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, 40000, 1))

        # update port reference
        self.assertFalse(main_node.connection_pool.has_connection(LOCALHOST, 50000))
        self.assertTrue(main_node.connection_pool.has_connection(LOCALHOST, 40000))
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, 40001))

        peer_outbound_connection.msg_ack(AckMessage())

        self.assertEqual(ConnectionState.ESTABLISHED, main_inbound_connection.state)
        self.assertEqual(ConnectionState.ESTABLISHED, peer_outbound_connection.state)

    def test_msg_hello_reject_mismatched_ip(self):
        fileno = 1

        inbound_connection = self._create_connection(fileno, LOCALHOST, 40000, False)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 40000))

        inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                         DEFAULT_NETWORK_NUM, "192.168.1.1", 8001, 1))

        self.assertTrue(inbound_connection.state & ConnectionState.MARK_FOR_CLOSE)

    def test_msg_hello_replace_other_connection(self):
        inbound_fileno = 1
        inbound_connection = self._create_connection(inbound_fileno, LOCALHOST, 40000, False)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 40000))

        outbound_fileno = 2
        outbound_connection = self._create_connection(outbound_fileno, LOCALHOST, 8001, True)
        outbound_connection.ordering = 1
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))

        inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                         DEFAULT_NETWORK_NUM, LOCALHOST, 8001, 10))

        self.assertTrue(outbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertFalse(self.node.connection_pool.has_connection(LOCALHOST, 40000))
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))

    def test_msg_hello_replace_self(self):
        inbound_fileno = 1
        inbound_connection = self._create_connection(inbound_fileno, LOCALHOST, 40000, False)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 40000))

        outbound_fileno = 2
        outbound_connection = self._create_connection(outbound_fileno, LOCALHOST, 8001, True)
        outbound_connection.ordering = 10
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))

        inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                         DEFAULT_NETWORK_NUM, LOCALHOST, 8001, 1))

        self.assertTrue(inbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))
        self.assertEqual(outbound_connection, self.node.connection_pool.get_by_ipport(LOCALHOST, 8001))

    def test_msg_hello_calls_on_two_nodes_resolve(self):
        main_port = 8000
        peer_port = 8001

        main_inbound_fileno = 1
        main_inbound_port = 40000
        main_inbound_connection = self._create_connection(main_inbound_fileno, LOCALHOST, main_inbound_port, False)
        self.assertEquals(GatewayConnection.NULL_ORDERING, main_inbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, main_inbound_port))

        main_outbound_fileno = 2
        main_outbound_ordering = 11
        main_outbound_connection = self._create_connection(main_outbound_fileno, LOCALHOST, peer_port, True)
        main_outbound_connection.ordering = main_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, main_outbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, peer_port))

        peer_node = create_node(LOCALHOST, peer_port)

        peer_inbound_fileno = 1
        peer_inbound_port = 40001
        peer_inbound_connection = self._create_connection(peer_inbound_fileno, LOCALHOST, peer_inbound_port, False,
                                                          node=peer_node)
        self.assertEquals(GatewayConnection.NULL_ORDERING, peer_inbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, peer_inbound_port))

        peer_outbound_fileno = 2
        peer_outbound_ordering = 10
        peer_outbound_connection = self._create_connection(peer_outbound_fileno, LOCALHOST, main_port, True,
                                                           node=peer_node)
        peer_outbound_connection.ordering = peer_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, peer_outbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, main_port))

        main_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, peer_port,
                                                              peer_outbound_ordering))
        peer_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, main_port,
                                                              main_outbound_ordering))

        self.assertTrue(main_inbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertTrue(peer_outbound_connection.state & ConnectionState.MARK_FOR_CLOSE)

    def test_msg_hello_calls_on_two_nodes_retry(self):
        main_port = 8000
        peer_port = 8001

        main_inbound_fileno = 1
        main_inbound_port = 40000
        main_inbound_connection = self._create_connection(main_inbound_fileno, LOCALHOST, main_inbound_port, False)
        main_inbound_connection.enqueue_msg = MagicMock()
        self.assertEquals(GatewayConnection.NULL_ORDERING, main_inbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, main_inbound_port))

        main_outbound_fileno = 2
        main_outbound_ordering = 10
        main_outbound_connection = self._create_connection(main_outbound_fileno, LOCALHOST, peer_port, True)
        main_outbound_connection.ordering = main_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, main_outbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, peer_port))

        peer_node = create_node(LOCALHOST, peer_port)

        peer_inbound_fileno = 1
        peer_inbound_port = 40001
        peer_inbound_connection = self._create_connection(peer_inbound_fileno, LOCALHOST, peer_inbound_port, False,
                                                          node=peer_node)
        peer_inbound_connection.enqueue_msg = MagicMock()
        self.assertEquals(GatewayConnection.NULL_ORDERING, peer_inbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, peer_inbound_port))

        peer_outbound_fileno = 2
        peer_outbound_ordering = 10
        peer_outbound_connection = self._create_connection(peer_outbound_fileno, LOCALHOST, main_port, True,
                                                           node=peer_node)
        peer_outbound_connection.ordering = peer_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, peer_outbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, main_port))

        main_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, peer_port,
                                                              peer_outbound_ordering))
        peer_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, main_port,
                                                              main_outbound_ordering))

        self.assertFalse(main_inbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertFalse(main_outbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertFalse(peer_inbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertFalse(peer_outbound_connection.state & ConnectionState.MARK_FOR_CLOSE)

        main_inbound_connection.enqueue_msg.assert_called_once()
        peer_inbound_connection.enqueue_msg.assert_called_once()

        main_inbound_connection.ordering = 11
        peer_inbound_connection.ordering = 12

        main_outbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                               DEFAULT_NETWORK_NUM, LOCALHOST, peer_port,
                                                               peer_inbound_connection.ordering))
        peer_outbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                               DEFAULT_NETWORK_NUM, LOCALHOST, main_port,
                                                               main_inbound_connection.ordering))

        self.assertTrue(main_inbound_connection.state & ConnectionState.MARK_FOR_CLOSE)
        self.assertTrue(peer_outbound_connection.state & ConnectionState.MARK_FOR_CLOSE)

    def test_msg_block_propagation_request(self):
        block = helpers.generate_bytearray(30)
        message = BlockPropagationRequestMessage(block)

        self.connection.msg_block_propagation_request(message)
        self.node.neutrality_service.propagate_block_to_network.assert_called_once()

    def test_msg_block_hold(self):
        block_hash = Sha256Hash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN))
        message = BlockHoldingMessage(block_hash, network_num=123)
        self.connection.msg_block_holding(message)
        self.node.block_processing_service.place_hold.assert_called_once_with(block_hash, self.connection)
