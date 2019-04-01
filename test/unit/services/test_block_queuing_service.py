import time

from mock import MagicMock, Mock

from bxcommon.constants import MISSING_BLOCK_EXPIRE_TIME, HDR_COMMON_OFF
from bxcommon.messages.bloxroute.message import Message
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import Sha256ObjectHash
from bxgateway.gateway_constants import MIN_INTERVAL_BETWEEN_BLOCKS_S, NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S
from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class BlockQueuingServiceTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000))

        self.node_connection = Mock()
        self.node_connection.is_active = MagicMock(return_value=True)

        self.node.node_conn = self.node_connection

        self.block_queuing_service = BlockQueuingService(self.node)

    def test_push_arguments_validation(self):
        self.assertRaises(TypeError, self.block_queuing_service.push, helpers.generate_bytearray(SHA256_HASH_LEN))
        self.assertRaises(ValueError, self.block_queuing_service.push,
                          Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN)), None, False)

    def test_remove_arguments_validation(self):
        self.assertRaises(TypeError, self.block_queuing_service.remove, helpers.generate_bytearray(SHA256_HASH_LEN))

    def test_update_recovered_block_arguments_validation(self):
        self.assertRaises(TypeError, self.block_queuing_service.update_recovered_block,
                          helpers.generate_bytearray(SHA256_HASH_LEN), Mock())
        self.assertRaises(ValueError, self.block_queuing_service.update_recovered_block,
                          Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN)), None)

    def test_block_added_to_empty_queue(self):
        block_hash = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg = self._create_dummy_message()

        self.block_queuing_service.push(block_hash, block_msg, waiting_for_recovery=False)

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))

    def test_block_added_when_node_is_not_ready(self):
        self.node.node_conn = None

        block_hash = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg = self._create_dummy_message()

        self.block_queuing_service.push(block_hash, block_msg, waiting_for_recovery=False)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        time.time = MagicMock(return_value=time.time() + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S)
        self.node.alarm_queue.fire_alarms()

        # verify that item is still in the queue if node connection is still not available
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        self.node.node_conn = Mock()
        self.node.node_conn.is_active = MagicMock(return_value=False)

        time.time = MagicMock(return_value=time.time() + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S * 2)
        self.node.alarm_queue.fire_alarms()

        # verify that item is still in the queue if node connection is available but not active
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        self.node.node_conn.is_active = MagicMock(return_value=True)

        time.time = MagicMock(return_value=time.time() + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S * 3)
        self.node.alarm_queue.fire_alarms()

        # verify that is sent to node once connection to node is active
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))

    def test_waiting_recovery_block_to_empty_queue(self):
        block_hash = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))

        self.block_queuing_service.push(block_hash, None, waiting_for_recovery=True)
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

    def test_waiting_recovery__timeout(self):
        block_hash1 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))

        block_hash2 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg2 = self._create_dummy_message()

        block_hash3 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg3 = self._create_dummy_message()

        self.block_queuing_service.push(block_hash1, None, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash2, block_msg2, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash3, block_msg3, waiting_for_recovery=False)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))

        # fire alarm when block recovery not expired
        time.time = MagicMock(return_value=time.time() + MISSING_BLOCK_EXPIRE_TIME / 2)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))

        # fire alarm after recovery expires and is not successful
        time.time = MagicMock(return_value=time.time() + MISSING_BLOCK_EXPIRE_TIME)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

    def test_last_block_is_recovered(self):
        block_hash1 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg1 = self._create_dummy_message()

        block_hash2 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg2 = self._create_dummy_message()

        block_hash3 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg3 = self._create_dummy_message()

        self.block_queuing_service.push(block_hash1, block_msg1, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash2, block_msg2, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash3, block_msg3, waiting_for_recovery=True)

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))
        self.assertEqual(block_msg1, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        time.time = MagicMock(return_value=time.time() + MIN_INTERVAL_BETWEEN_BLOCKS_S)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        time.time = MagicMock(return_value=time.time() + 2 * MIN_INTERVAL_BETWEEN_BLOCKS_S)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        self.block_queuing_service.update_recovered_block(block_hash3, block_msg3)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg3, self.node.send_to_node_messages[0])

    def test_top_block_is_recovered(self):
        block_hash1 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg1 = self._create_dummy_message()

        block_hash2 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg2 = self._create_dummy_message()

        block_hash3 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg3 = self._create_dummy_message()

        self.block_queuing_service.push(block_hash1, None, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash2, block_msg2, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash3, block_msg3, waiting_for_recovery=False)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))

        self.block_queuing_service.update_recovered_block(block_hash1, block_msg1)
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))
        self.assertEqual(block_msg1, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        self.node.alarm_queue.fire_alarms()
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))

        time.time = MagicMock(return_value=time.time() + MIN_INTERVAL_BETWEEN_BLOCKS_S)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        time.time = MagicMock(return_value=time.time() + 2 * MIN_INTERVAL_BETWEEN_BLOCKS_S)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg3, self.node.send_to_node_messages[0])

    def test_waiting_recovery_top_block_is_removed(self):
        block_hash1 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))

        block_hash2 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg2 = self._create_dummy_message()

        block_hash3 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg3 = self._create_dummy_message()

        self.block_queuing_service.push(block_hash1, None, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash2, block_msg2, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash3, block_msg3, waiting_for_recovery=False)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))

        self.block_queuing_service.remove(block_hash1)
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        time.time = MagicMock(return_value=time.time() + MIN_INTERVAL_BETWEEN_BLOCKS_S)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg3, self.node.send_to_node_messages[0])

    def test_waiting_recovery_last_block_is_removed(self):
        block_hash1 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg1 = self._create_dummy_message()

        block_hash2 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg2 = self._create_dummy_message()

        block_hash3 = Sha256ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        block_msg3 = self._create_dummy_message()

        self.block_queuing_service.push(block_hash1, block_msg1, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash2, block_msg2, waiting_for_recovery=False)
        self.block_queuing_service.push(block_hash3, block_msg3, waiting_for_recovery=True)

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))
        self.assertEqual(block_msg1, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        self.block_queuing_service.remove(block_hash3)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        self.node.alarm_queue.fire_alarms()

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        time.time = MagicMock(return_value=time.time() + MIN_INTERVAL_BETWEEN_BLOCKS_S)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

    def _create_dummy_message(self):
        return Message(msg_type=b"dummy", payload_len=SHA256_HASH_LEN,
                       buf=bytearray(helpers.generate_bytearray(HDR_COMMON_OFF + SHA256_HASH_LEN)))
