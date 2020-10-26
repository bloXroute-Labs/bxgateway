import time
from typing import cast
from mock import MagicMock

from bxcommon.constants import LOCALHOST
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.services.abstract_block_queuing_service import BlockQueueEntry
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class BlockQueuingServiceManagerTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000)
        )
        self.node_conn = cast(
            AbstractGatewayBlockchainConnection,
            MockConnection(MockSocketConnection(1, self.node, ip_address=LOCALHOST, port=8002), self.node)
        )
        self.node.blockchain_peers.add(BlockchainPeerInfo(self.node_conn.peer_ip, self.node_conn.peer_port))

        self.node_conn_2 = cast(
            AbstractGatewayBlockchainConnection,
            MockConnection(MockSocketConnection(2, self.node, ip_address=LOCALHOST, port=8003), self.node)
        )
        self.node.blockchain_peers.add(BlockchainPeerInfo(self.node_conn_2.peer_ip, self.node_conn_2.peer_port))

        self.node_conn_3 = cast(
            AbstractGatewayBlockchainConnection,
            MockConnection(MockSocketConnection(3, self.node, ip_address=LOCALHOST, port=8004), self.node)
        )
        self.node.blockchain_peers.add(BlockchainPeerInfo(self.node_conn_3.peer_ip, self.node_conn_3.peer_port))

    def test_add_queuing_service(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service[self.node_conn]
        )
        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.designated_queuing_service
        )

    def test_get_designated_block_queuing_service(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)

        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.get_designated_block_queuing_service()
        )

    def test_remove_only_block_queuing_service(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service[self.node_conn]
        )
        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.designated_queuing_service
        )
        self.node.block_queuing_service_manager.remove_block_queuing_service(self.node_conn)
        self.assertNotIn(self.node_conn, self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service)
        self.assertIsNone(self.node.block_queuing_service_manager.designated_queuing_service)

    def test_add_multiple_block_queuing_services(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)

        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service[self.node_conn]
        )
        self.assertEqual(
            block_queuing_service_2,
            self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service[self.node_conn_2]
        )
        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.designated_queuing_service
        )

    def test_remove_second_block_queuing_services(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)

        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.designated_queuing_service
        )

        self.node.block_queuing_service_manager.remove_block_queuing_service(self.node_conn)
        self.assertNotIn(self.node_conn, self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service)
        self.assertEqual(
            block_queuing_service_2,
            self.node.block_queuing_service_manager.designated_queuing_service
        )

    def test_get_block_queuing_service(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)

        self.assertEqual(
            block_queuing_service,
            self.node.block_queuing_service_manager.get_block_queuing_service(self.node_conn)
        )
        self.assertEqual(
            block_queuing_service_2,
            self.node.block_queuing_service_manager.get_block_queuing_service(self.node_conn_2)
        )

    def test_push_block_queuing_service(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service.push = MagicMock()
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)
        block_queuing_service_2.push = MagicMock()
        block_queuing_service_3 = self.node.build_block_queuing_service(self.node_conn_3)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_3, block_queuing_service_3)
        block_queuing_service_3.push = MagicMock()

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)
        self.node.block_queuing_service_manager.push(
            block_hash, block_msg
        )
        block_queuing_service.push.assert_called_with(block_hash, block_msg, False)
        block_queuing_service_2.push.assert_called_with(block_hash, block_msg, False)
        block_queuing_service_3.push.assert_called_with(block_hash, block_msg, False)

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = helpers.create_block_message(block_hash)
        self.node.block_queuing_service_manager.push(
            block_hash2, block_msg2, waiting_for_recovery=True
        )
        block_queuing_service.push.assert_called_with(block_hash2, block_msg2, True)
        block_queuing_service_2.push.assert_called_with(block_hash2, block_msg2, True)
        block_queuing_service_3.push.assert_called_with(block_hash2, block_msg2, True)

    def test_store_block_data(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)

        self.node.block_queuing_service_manager.store_block_data(block_hash, block_msg)
        self.assertIn(block_hash, self.node.block_storage)
        self.assertEqual(block_msg, self.node.block_storage[block_hash])

    def test_get_block_data(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)

        self.node.block_queuing_service_manager.store_block_data(block_hash, block_msg)
        self.assertEqual(block_msg, self.node.block_queuing_service_manager.get_block_data(block_hash))

    def test_remove_block(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service.remove = MagicMock()
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)
        block_queuing_service_2.remove = MagicMock()

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)

        self.node.block_queuing_service_manager.store_block_data(block_hash, block_msg)
        block_queuing_service._blocks.add(block_hash)
        block_queuing_service_2._blocks.add(block_hash)
        self.assertIn(block_hash, self.node.block_storage)
        self.assertEqual(block_msg, self.node.block_storage[block_hash])

        self.node.block_queuing_service_manager.remove(block_hash)
        self.assertNotIn(block_hash, self.node.block_storage)
        block_queuing_service.remove.assert_called_with(block_hash)
        block_queuing_service_2.remove.assert_called_with(block_hash)

    def test_update_recovered_block(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service.update_recovered_block = MagicMock()
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)
        block_queuing_service_2.update_recovered_block = MagicMock()
        block_queuing_service_3 = self.node.build_block_queuing_service(self.node_conn_3)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_3, block_queuing_service_3)
        block_queuing_service_3.update_recovered_block = MagicMock()
        self.assertEqual(len(self.node.block_queuing_service_manager.blockchain_peer_to_block_queuing_service), 3)

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)
        self.node.block_queuing_service_manager.update_recovered_block(
            block_hash, block_msg
        )
        block_queuing_service.update_recovered_block.assert_called_with(block_hash, block_msg)
        block_queuing_service_2.update_recovered_block.assert_called_with(block_hash, block_msg)
        block_queuing_service_3.update_recovered_block.assert_called_with(block_hash, block_msg)

        self.assertIn(block_hash, self.node.block_storage)
        self.assertEqual(block_msg, self.node.block_storage[block_hash])

    def test_get_length_of_each_queuing_service_stats_format(self):
        block_queuing_service = self.node.build_block_queuing_service(self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, block_queuing_service)
        block_queuing_service_2 = self.node.build_block_queuing_service(self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, block_queuing_service_2)

        block_hash = Sha256Hash(helpers.generate_hash())
        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_queuing_service._block_queue.append(BlockQueueEntry(block_hash, time.time()))
        block_queuing_service._block_queue.append(BlockQueueEntry(block_hash2, time.time()))
        block_queuing_service._block_queue.append(BlockQueueEntry(block_hash3, time.time()))
        block_queuing_service_2._block_queue.append(BlockQueueEntry(block_hash, time.time()))

        stats = self.node.block_queuing_service_manager.get_length_of_each_queuing_service_stats_format()
        self.assertEqual(stats, "[127.0.0.1 8002: 3, 127.0.0.1 8003: 1]")
