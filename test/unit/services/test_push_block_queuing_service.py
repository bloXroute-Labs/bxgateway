import time
from typing import List, Optional

from mock import MagicMock

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.test_utils.helpers import TestBlockMessage
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway.testing import gateway_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.test_utils import helpers
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import gateway_constants
from bxgateway.gateway_constants import (
    MAX_INTERVAL_BETWEEN_BLOCKS_S,
    NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S,
    BLOCK_RECOVERY_MAX_QUEUE_TIME,
    BLOCK_QUEUE_LENGTH_LIMIT,
    MAX_BLOCK_CACHE_TIME_S,
    LOCALHOST)
from bxgateway.services.push_block_queuing_service import (
    PushBlockQueuingService,
)
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.blockchain_peer_info import BlockchainPeerInfo


class TestBlockHeaderMessage(AbstractMessage):
    def __init__(self, block_hash: Sha256Hash):
        self.block_hash = block_hash
        self._rawbytes = memoryview(self.block_hash.binary)

    @classmethod
    def unpack(cls, buf):
        pass

    @classmethod
    def validate_payload(cls, buf, unpacked_args):
        pass

    @classmethod
    def initialize_class(cls, cls_type, buf, unpacked_args):
        pass

    def rawbytes(self) -> memoryview:
        return self._rawbytes


class TestPushBlockQueuingService(
    PushBlockQueuingService[TestBlockMessage, TestBlockHeaderMessage]
):
    def __init__(self, node, node_conn):
        super().__init__(node, node_conn)
        self.blocks_sent: List[Sha256Hash] = []

    def build_block_header_message(
        self, block_hash: Sha256Hash, _block_message: TestBlockMessage
    ) -> TestBlockHeaderMessage:
        return TestBlockHeaderMessage(block_hash)

    def get_previous_block_hash_from_message(
        self, block_message: TestBlockMessage
    ) -> Sha256Hash:
        return block_message.previous_block

    def on_block_sent(
        self, block_hash: Sha256Hash, _block_message: TestBlockMessage
    ):
        self.blocks_sent.append(block_hash)


TTL = 300


class BlockQueuingServiceTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(
                8000,
                max_block_interval_s=gateway_constants.MAX_INTERVAL_BETWEEN_BLOCKS_S,
                blockchain_message_ttl=TTL
            )
        )
        self.node_conn = MockConnection(MockSocketConnection(
            1, self.node, ip_address=LOCALHOST, port=8002), self.node
        )
        self.node.blockchain_peers.add(BlockchainPeerInfo(self.node_conn.peer_ip, self.node_conn.peer_port))
        self.block_queuing_service = TestPushBlockQueuingService(self.node, self.node_conn)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn, self.block_queuing_service)
        self.node_conn.enqueue_msg = MagicMock()

        self.node_conn_2 = MockConnection(MockSocketConnection(
            2, self.node, ip_address=LOCALHOST, port=8003), self.node
        )
        self.node.blockchain_peers.add(BlockchainPeerInfo(self.node_conn_2.peer_ip, self.node_conn_2.peer_port))
        self.block_queuing_service_2 = TestPushBlockQueuingService(self.node, self.node_conn_2)
        self.node.block_queuing_service_manager.add_block_queuing_service(self.node_conn_2, self.block_queuing_service_2)
        self.node_conn_2.enqueue_msg = MagicMock()

        self.blockchain_connections = [self.node_conn, self.node_conn_2]
        self.block_queuing_services = [self.block_queuing_service, self.block_queuing_service_2]

    def test_block_added_to_empty_queue(self):
        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)

        self.node.block_queuing_service_manager.push(
            block_hash, block_msg, waiting_for_recovery=False
        )

        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg, 0)

        self._assert_len_of_block_queuing_services(0)
        self._assert_len_of_blocks_sent(1)
        self.assertEqual(block_hash, self.block_queuing_service.blocks_sent[0])

        # block still in service after confirmation
        self._assert_block_in_queuing_services(block_hash)
        self._mark_block_seen_by_blockchain_nodes(
            block_hash, None
        )
        self._assert_block_in_queuing_services(block_hash)

    def test_block_added_when_node_is_not_ready(self):
        self.node.has_active_blockchain_peer = MagicMock(return_value=False)

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = helpers.create_block_message(block_hash)

        self.node.block_queuing_service_manager.push(
            block_hash, block_msg, waiting_for_recovery=False
        )

        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_queuing_services(block_hash)

        time.time = MagicMock(
            return_value=time.time()
            + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S
        )
        self.node.alarm_queue.fire_alarms()

        # verify that item is still in the queue if node connection
        # is still not available
        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_queuing_services(block_hash)

        self.node.has_active_blockchain_peer = MagicMock(return_value=True)

        time.time = MagicMock(
            return_value=time.time()
            + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S * 2
        )
        self.node.alarm_queue.fire_alarms()

        # verify that is sent to node once connection to node is active
        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg, 0)
        self._assert_len_of_block_queuing_services(0)

        # verify item still cached, since confirmation not received
        self._assert_block_in_queuing_services(block_hash)

    def test_block_added_waiting_for_previous_block_confirmation(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = helpers.create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = helpers.create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.node.block_queuing_service_manager.push(block_hash_1, block_msg_1)
        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_1, 0)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_queuing_services(block_hash_1)

        # no confirmation, wait on block 1
        self.node.block_queuing_service_manager.push(block_hash_2, block_msg_2)
        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_1, 0)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_queuing_services(block_hash_1)
        self._assert_block_in_queuing_services(block_hash_2)

        self._mark_blocks_seen_by_blockchain_nodes(
            [block_hash_1]
        )
        self._assert_length_of_enqueued_messages(2)
        self._assert_block_in_enqueued_messages_index(block_msg_2, 1)
        self._assert_len_of_block_queuing_services(0)

    def test_block_added_send_immediately_with_confirmation(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = helpers.create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = helpers.create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.node.block_queuing_service_manager.push(block_hash_1, block_msg_1)
        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_1, 0)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_queuing_services(block_hash_1)

        # confirmed, send immediately
        self._mark_block_seen_by_blockchain_nodes(
            block_hash_1, None
        )
        self.node.block_queuing_service_manager.push(block_hash_2, block_msg_2)
        self._assert_length_of_enqueued_messages(2)
        self._assert_block_in_enqueued_messages_index(block_msg_2, 1)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_queuing_services(block_hash_1)
        self._assert_block_in_queuing_services(block_hash_2)

    def test_block_removed_with_ttl_check(self):
        self.node.has_active_blockchain_peer = MagicMock(return_value=False)

        block_hash_start = Sha256Hash(helpers.generate_hash())
        block_msg_start = helpers.create_block_message(block_hash_start)
        block_hash_end = Sha256Hash(helpers.generate_hash())
        block_msg_end = helpers.create_block_message(block_hash_end)
        self.node.block_queuing_service_manager.push(block_hash_start, block_msg_start)

        for i in range(2, BLOCK_QUEUE_LENGTH_LIMIT + 1):
            block_hash = Sha256Hash(helpers.generate_hash())
            block_msg = helpers.create_block_message(block_hash)
            self.node.block_queuing_service_manager.push(block_hash, block_msg)

        self.assertEqual(BLOCK_QUEUE_LENGTH_LIMIT, len(self.block_queuing_service))
        self._assert_length_of_enqueued_messages(0)

        self.node.has_active_blockchain_peer = MagicMock(return_value=True)
        time.time = MagicMock(
            return_value=time.time()
            + MAX_BLOCK_CACHE_TIME_S * 2
        )
        self.assertEqual(BLOCK_QUEUE_LENGTH_LIMIT, len(self.block_queuing_service._block_queue))
        self.node.block_queuing_service_manager.push(block_hash_end, block_msg_end)
        self._mark_blocks_seen_by_blockchain_nodes([block_hash_end])
        self.assertEqual(0, len(self.block_queuing_service._block_queue))

    def test_waiting_confirmation_timeout(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = helpers.create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = helpers.create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.node.block_queuing_service_manager.push(block_hash_1, block_msg_1)
        self.node.block_queuing_service_manager.push(block_hash_2, block_msg_2)

        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_1, 0)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_queuing_services(block_hash_1)
        self._assert_block_in_queuing_services(block_hash_2)

        time.time = MagicMock(
            return_value=time.time()
            + gateway_constants.MAX_INTERVAL_BETWEEN_BLOCKS_S
        )
        self.node.alarm_queue.fire_alarms()

        self._assert_length_of_enqueued_messages(2)
        self._assert_block_in_enqueued_messages_index(block_msg_2, 1)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_queuing_services(block_hash_1)
        self._assert_block_in_queuing_services(block_hash_2)

    def test_waiting_recovery_block_to_empty_queue(self):
        block_hash = Sha256Hash(helpers.generate_hash())

        self.node.block_queuing_service_manager.push(
            block_hash, None, waiting_for_recovery=True
        )
        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_queuing_services(block_hash)

    def test_waiting_recovery__timeout(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = helpers.create_block_message(block_hash2)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = helpers.create_block_message(block_hash3)

        self.node.block_queuing_service_manager.push(
            block_hash1, None, waiting_for_recovery=True
        )
        self.node.block_queuing_service_manager.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.node.block_queuing_service_manager.push(
            block_hash3, block_msg3, waiting_for_recovery=False
        )

        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(3)
        self._assert_block_in_queuing_services(block_hash1)
        self._assert_block_in_queuing_services(block_hash2)
        self._assert_block_in_queuing_services(block_hash3)

        # fire alarm when block recovery not expired
        time.time = MagicMock(
            return_value=time.time() + BLOCK_RECOVERY_MAX_QUEUE_TIME / 2 + 1
        )
        self.node.alarm_queue.fire_alarms()
        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(3)
        self._assert_block_in_queuing_services(block_hash1)
        self._assert_block_in_queuing_services(block_hash2)
        self._assert_block_in_queuing_services(block_hash3)

        # fire alarm after recovery expires and is not successful
        time.time = MagicMock(
            return_value=time.time() + BLOCK_RECOVERY_MAX_QUEUE_TIME
        )
        self.node.alarm_queue.fire_alarms()
        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_enqueued_messages_index(block_msg2, 0)
        self.assertNotIn(block_hash1, self.block_queuing_service)
        self._assert_block_in_queuing_services(block_hash2)
        self._assert_block_in_queuing_services(block_hash3)

    def test_last_block_is_recovered(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())
        block_msg1 = helpers.create_block_message()

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = helpers.create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = helpers.create_block_message(block_hash3, block_hash2)

        self.node.block_queuing_service_manager.push(
            block_hash1, block_msg1, waiting_for_recovery=False
        )
        self._mark_block_seen_by_blockchain_nodes(
            block_hash1, None
        )
        self.node.block_queuing_service_manager.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self._mark_block_seen_by_blockchain_nodes(
            block_hash2, None
        )
        self.node.block_queuing_service_manager.push(
            block_hash3, block_msg3, waiting_for_recovery=True
        )

        self._assert_length_of_enqueued_messages(2)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_enqueued_messages_index(block_msg1, 0)
        self._assert_block_in_enqueued_messages_index(block_msg2, 1)

        self._reset_enqueue_msg_mocks()
        self._reset_enqueue_msg_mocks()

        # don't send, since not recovered enough though timeout
        time.time = MagicMock(
            return_value=time.time() + MAX_INTERVAL_BETWEEN_BLOCKS_S
        )
        self.node.alarm_queue.fire_alarms()
        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(1)

        self.node.block_queuing_service_manager.update_recovered_block(
            block_hash3, block_msg3
        )
        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_enqueued_messages_index(block_msg3, 0)

    def test_top_block_is_recovered(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())
        block_msg1 = helpers.create_block_message(block_hash1)

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = helpers.create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = helpers.create_block_message(block_hash3, block_hash2)

        self.node.block_queuing_service_manager.push(
            block_hash1, None, waiting_for_recovery=True
        )
        self.node.block_queuing_service_manager.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.node.block_queuing_service_manager.push(
            block_hash3, block_msg3, waiting_for_recovery=False
        )

        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(3)

        self.node.block_queuing_service_manager.update_recovered_block(
            block_hash1, block_msg1
        )
        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(2)
        self._assert_block_in_enqueued_messages_index(block_msg1, 0)

        self._reset_enqueue_msg_mocks()

        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(2)

        self._mark_blocks_seen_by_blockchain_nodes(
            [block_hash1]
        )
        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_enqueued_messages_index(block_msg2, 0)

        self._reset_enqueue_msg_mocks()

        self._mark_blocks_seen_by_blockchain_nodes(
            [block_hash2]
        )
        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_enqueued_messages_index(block_msg3, 0)

    def test_waiting_recovery_top_block_timeout(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = helpers.create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = helpers.create_block_message(block_hash3, block_hash2)

        self.node.block_queuing_service_manager.push(
            block_hash1, None, waiting_for_recovery=True
        )
        self.node.block_queuing_service_manager.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.node.block_queuing_service_manager.push(
            block_hash3, block_msg3, waiting_for_recovery=False
        )

        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(3)
        self._assert_block_in_queuing_services(block_hash1)
        self._assert_block_in_queuing_services(block_hash2)
        self._assert_block_in_queuing_services(block_hash3)

        # trigger automatic removal of top block recovery
        time.time = MagicMock(
            return_value=time.time()
            + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME
        )
        self.node.alarm_queue.fire_alarms()

        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(1)
        self._assert_block_in_enqueued_messages_index(block_msg2, 0)

        self._reset_enqueue_msg_mocks()
        self._mark_blocks_seen_by_blockchain_nodes(
            [block_hash2]
        )

        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_enqueued_messages_index(block_msg3, 0)

    def test_waiting_recovery_last_block_is_removed(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())
        block_msg1 = helpers.create_block_message(block_hash1)

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = helpers.create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = helpers.create_block_message(block_hash3, block_hash2)

        self.node.block_queuing_service_manager.push(
            block_hash1, block_msg1, waiting_for_recovery=False
        )
        self.node.block_queuing_service_manager.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.node.block_queuing_service_manager.push(
            block_hash3, block_msg3, waiting_for_recovery=True
        )

        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(2)
        self._assert_block_in_enqueued_messages_index(block_msg1, 0)

        self._reset_enqueue_msg_mocks()

        self.block_queuing_service.remove(block_hash3)
        self.block_queuing_service_2.remove(block_hash3)

        self._assert_length_of_enqueued_messages(0)
        self._assert_len_of_block_queuing_services(1)

        self._mark_blocks_seen_by_blockchain_nodes(
            [block_hash1]
        )
        self._assert_length_of_enqueued_messages(1)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_enqueued_messages_index(block_msg2, 0)

    def test_sending_headers_to_node(self):
        block_hash = Sha256Hash(helpers.generate_hash())
        block_message = helpers.create_block_message(block_hash)
        self.node.block_queuing_service_manager.push(block_hash, block_message)
        success = self.block_queuing_service.try_send_header_to_node(block_hash)
        self.assertTrue(success)
        success = self.block_queuing_service_2.try_send_header_to_node(block_hash)
        self.assertTrue(success)

        self._assert_length_of_enqueued_messages(2)

        block_header_message = self._get_enqueued_messages(self.node_conn)[1]
        self.assertIsInstance(block_header_message, TestBlockHeaderMessage)
        self.assertEqual(block_hash, block_header_message.block_hash)
        block_header_message = self._get_enqueued_messages(self.node_conn_2)[1]
        self.assertIsInstance(block_header_message, TestBlockHeaderMessage)
        self.assertEqual(block_hash, block_header_message.block_hash)

    def test_sending_headers_to_node_recovery(self):
        block_hash = Sha256Hash(helpers.generate_hash())
        self.node.block_queuing_service_manager.push(
            block_hash, None, waiting_for_recovery=True
        )
        success = self.block_queuing_service.try_send_header_to_node(block_hash)
        self.assertFalse(success)
        self._assert_length_of_enqueued_messages(0)

    def test_sending_block_timed_out(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = helpers.create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = helpers.create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.node.block_queuing_service_manager.push(block_hash_1, block_msg_1)
        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_1, 0)
        self._assert_len_of_block_queuing_services(0)
        self._assert_block_in_queuing_services(block_hash_1)

        self.node.block_queuing_service_manager.push(block_hash_2, block_msg_2)

        time.time = MagicMock(return_value=time.time() + 350)

        # too much time has elapsed, message is dropped
        self.node.alarm_queue.fire_alarms()
        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_1, 0)
        self._assert_len_of_block_queuing_services(0)

    def test_block_queue_continues_after_timeout(self):
        block_hash_1 = helpers.generate_object_hash()
        block_msg_1 = helpers.create_block_message(block_hash_1)

        block_hash_2 = helpers.generate_object_hash()
        block_msg_2 = helpers.create_block_message(block_hash_2, block_hash_1)

        block_hash_3 = helpers.generate_object_hash()
        block_msg_3 = helpers.create_block_message(block_hash_3, block_hash_2)

        self.node.has_active_blockchain_peer = MagicMock(return_value=False)

        self.node.block_queuing_service_manager.push(block_hash_1, block_msg_1)
        time.time = MagicMock(return_value=time.time() + 150)

        self.node.block_queuing_service_manager.push(block_hash_2, block_msg_2)
        self.node.block_queuing_service_manager.push(block_hash_3, block_msg_3)
        time.time = MagicMock(return_value=time.time() + TTL - 150 + 1)

        # too much time has elapsed for the first message
        self.node.has_active_blockchain_peer = MagicMock(return_value=True)
        self.node.alarm_queue.fire_alarms()

        self._assert_length_of_enqueued_messages(1)
        self._assert_block_in_enqueued_messages_index(block_msg_2, 0)

    def _get_enqueued_messages(self, connection):
        calls = connection.enqueue_msg.call_args_list
        enqueue_msg_calls = []
        for call in calls:
            ((msg,), _) = call
            enqueue_msg_calls.append(msg)
        return enqueue_msg_calls

    def _assert_length_of_enqueued_messages(self, expected_length: int):
        for connection in self.blockchain_connections:
            self.assertEqual(len(self._get_enqueued_messages(connection)), expected_length)

    def _assert_block_in_enqueued_messages_index(self, block_msg: AbstractBlockMessage, index: int):
        for connection in self.blockchain_connections:
            self.assertEqual(block_msg, self._get_enqueued_messages(connection)[index])

    def _mark_block_seen_by_blockchain_nodes(
        self, block_hash: Sha256Hash, block_msg: Optional[TestBlockMessage]
    ) -> None:
        for queuing_service in self.block_queuing_services:
            queuing_service.mark_block_seen_by_blockchain_node(block_hash, block_msg)
            
    def _mark_blocks_seen_by_blockchain_nodes(
        self, block_hashes: List[Sha256Hash]
    ) -> None:
        for queuing_service in self.block_queuing_services:
            for block_hash in block_hashes:
                queuing_service.mark_blocks_seen_by_blockchain_node([block_hash])
                
    def _assert_len_of_block_queuing_services(self, expected_length: int) -> None:
        for queuing_service in self.block_queuing_services:
            self.assertEqual(expected_length, len(queuing_service))

    def _assert_len_of_blocks_sent(self, expected_length: int) -> None:
        blocks_sent = self.block_queuing_service.blocks_sent
        for queuing_service in self.block_queuing_services:
            self.assertEqual(expected_length, len(queuing_service.blocks_sent))
            self.assertEqual(blocks_sent, queuing_service.blocks_sent)

    def _assert_block_in_queuing_services(self, block_hash: Sha256Hash):
        for queuing_service in self.block_queuing_services:
            self.assertIn(block_hash, queuing_service)
            
    def _reset_enqueue_msg_mocks(self) -> None:
        for connection in self.blockchain_connections:
            connection.enqueue_msg.reset_mock()
