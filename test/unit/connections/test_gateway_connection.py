from mock import MagicMock
from collections import defaultdict

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.connections.connection_pool import ConnectionPool
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import LOCALHOST, DEFAULT_NETWORK_NUM
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.test_utils import helpers
from bxcommon.utils import crypto
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_statistics_service import tx_stats

from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.gateway.block_propagation_request import BlockPropagationRequestMessage
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_version_manager import gateway_version_manager
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.blockchain_sync_service import BlockchainSyncService
from bxgateway.services.neutrality_service import NeutralityService


def create_node(ip=LOCALHOST, port=8000) -> AbstractGatewayNode:
    node = MagicMock(spec=AbstractGatewayNode)
    node.opts = helpers.get_gateway_opts(port, external_ip=ip)
    node.connection_pool = ConnectionPool()
    node.connection_exists = node.connection_pool.has_connection
    node.network_num = node.opts.blockchain_network_num
    node.in_progress_blocks = BlockEncryptedCache(AlarmQueue())
    node.neutrality_service = MagicMock(spec=NeutralityService)
    node.blockchain_sync_service = MagicMock(spec=BlockchainSyncService)
    node.block_processing_service = MagicMock(spec=BlockProcessingService)
    node.alarm_queue = None
    node.opts.log_detailed_block_stats = True
    node.opts.stats_calculate_actual_size = True
    block_stats.set_node(node)
    tx_stats.set_node(node)
    node.num_retries_by_ip = defaultdict(int)
    return node


def create_connection(port: int, node: AbstractGatewayNode, ip=LOCALHOST, **kwargs) -> GatewayConnection:
    connection = helpers.create_connection(GatewayConnection, node=node,
                                           node_opts=helpers.get_gateway_opts(node.opts.external_port, external_ip=ip,
                                                                              **kwargs),
                                           port=port,
                                           **kwargs)
    return connection


class GatewayConnectionTest(AbstractTestCase):
    def setUp(self):
        self.node = create_node()
        self.connection = create_connection(8000, self.node)

    def test_msg_hello_only_connection(self):
        main_node = create_node(LOCALHOST, 40001)
        peer_node = create_node(LOCALHOST, 40000)
        main_inbound_fileno = 1
        main_inbound_connection = create_connection(50000, main_node, file_no=main_inbound_fileno, from_me=False)
        peer_outbound_fileno = 2
        peer_outbound_connection = create_connection(40001, peer_node, file_no=peer_outbound_fileno, from_me=True)

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

    def test_msg_hello_not_rejecting_on_mismatched_ip(self):
        fileno = 1
        inbound_connection = create_connection(40000, self.node, from_me=False)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 40000))

        inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                         DEFAULT_NETWORK_NUM, "192.168.1.1", 8001, 1))

        self.assertTrue(inbound_connection.is_alive())

    def test_msg_hello_replace_other_connection(self):
        inbound_fileno = 1
        inbound_connection = create_connection(40000, self.node, file_no=inbound_fileno, from_me=False)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 40000))

        outbound_fileno = 2
        outbound_connection = create_connection(8001, self.node, file_no=outbound_fileno, from_me=True)
        outbound_connection.ordering = 1
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))

        inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                         DEFAULT_NETWORK_NUM, LOCALHOST, 8001, 10))

        self.assertFalse(outbound_connection.is_alive())
        self.assertFalse(self.node.connection_pool.has_connection(LOCALHOST, 40000))
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))

    def test_msg_hello_replace_self(self):
        inbound_fileno = 1
        inbound_connection = create_connection(40000, self.node, file_no=inbound_fileno, from_me=False)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 40000))

        outbound_fileno = 2
        outbound_connection = create_connection(8001, self.node, file_no=outbound_fileno, from_me=True)
        outbound_connection.ordering = 10
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))

        inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                         DEFAULT_NETWORK_NUM, LOCALHOST, 8001, 1))

        self.assertFalse(inbound_connection.is_alive())
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, 8001))
        self.assertEqual(outbound_connection, self.node.connection_pool.get_by_ipport(LOCALHOST, 8001))

    def test_msg_hello_calls_on_two_nodes_resolve(self):
        main_port = 8000
        peer_port = 8001

        main_inbound_fileno = 1
        main_inbound_port = 40000
        main_inbound_connection = create_connection(main_inbound_port, self.node, file_no=main_inbound_fileno,
                                                    from_me=False)
        self.assertEqual(GatewayConnection.NULL_ORDERING, main_inbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, main_inbound_port))

        main_outbound_fileno = 2
        main_outbound_ordering = 11
        main_outbound_connection = create_connection(peer_port, self.node, file_no=main_outbound_fileno,
                                                     from_me=True)
        main_outbound_connection.ordering = main_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, main_outbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, peer_port))

        peer_node = create_node(LOCALHOST, peer_port)

        peer_inbound_fileno = 1
        peer_inbound_port = 40001
        peer_inbound_connection = create_connection(peer_inbound_port, peer_node, file_no=peer_inbound_fileno,
                                                    from_me=False)
        self.assertEqual(GatewayConnection.NULL_ORDERING, peer_inbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, peer_inbound_port))

        peer_outbound_fileno = 2
        peer_outbound_ordering = 10
        peer_outbound_connection = create_connection(main_port, peer_node, file_no=peer_outbound_fileno,
                                                     from_me=True)
        peer_outbound_connection.ordering = peer_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, peer_outbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, main_port))

        main_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, peer_port,
                                                              peer_outbound_ordering))
        peer_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, main_port,
                                                              main_outbound_ordering))

        self.assertFalse(main_inbound_connection.is_alive())
        self.assertFalse(peer_outbound_connection.is_alive())

    def test_msg_hello_calls_on_two_nodes_retry(self):
        main_port = 8000
        peer_port = 8001

        main_inbound_fileno = 1
        main_inbound_port = 40000
        main_inbound_connection = create_connection(main_inbound_port, self.node, file_no=main_inbound_fileno,
                                                    from_me=False)
        main_inbound_connection.enqueue_msg = MagicMock()
        self.assertEqual(GatewayConnection.NULL_ORDERING, main_inbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, main_inbound_port))

        main_outbound_fileno = 2
        main_outbound_ordering = 10
        main_outbound_connection = create_connection(peer_port, self.node, file_no=main_outbound_fileno,
                                                     from_me=True)
        main_outbound_connection.ordering = main_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, main_outbound_connection.ordering)
        self.assertTrue(self.node.connection_pool.has_connection(LOCALHOST, peer_port))

        peer_node = create_node(LOCALHOST, peer_port)

        peer_inbound_fileno = 1
        peer_inbound_port = 40001
        peer_inbound_connection = create_connection(peer_inbound_port, peer_node, file_no=peer_inbound_fileno,
                                                    from_me=False)
        peer_inbound_connection.enqueue_msg = MagicMock()
        self.assertEqual(GatewayConnection.NULL_ORDERING, peer_inbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, peer_inbound_port))

        peer_outbound_fileno = 2
        peer_outbound_ordering = 10
        peer_outbound_connection = create_connection(main_port, peer_node, file_no=peer_outbound_fileno,
                                                     from_me=True)
        peer_outbound_connection.ordering = peer_outbound_ordering
        self.assertNotEqual(GatewayConnection.NULL_ORDERING, peer_outbound_connection.ordering)
        self.assertTrue(peer_node.connection_pool.has_connection(LOCALHOST, main_port))

        main_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, peer_port,
                                                              peer_outbound_ordering))
        peer_inbound_connection.msg_hello(GatewayHelloMessage(gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                                              DEFAULT_NETWORK_NUM, LOCALHOST, main_port,
                                                              main_outbound_ordering))

        self.assertTrue(main_inbound_connection.is_alive())
        self.assertTrue(main_outbound_connection.is_alive())
        self.assertTrue(peer_inbound_connection.is_alive())
        self.assertTrue(peer_outbound_connection.is_alive())

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

        self.assertFalse(main_inbound_connection.is_alive())
        self.assertFalse(peer_outbound_connection.is_alive())

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
