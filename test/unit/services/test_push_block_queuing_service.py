import time
from typing import List, Optional

from mock import MagicMock, Mock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.test_utils import helpers
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import gateway_constants
from bxgateway.gateway_constants import (
    MAX_INTERVAL_BETWEEN_BLOCKS_S,
    NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S,
    BLOCK_RECOVERY_MAX_QUEUE_TIME,
)
from bxgateway.services.push_block_queuing_service import (
    PushBlockQueuingService,
)
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class TestBlockMessage(AbstractMessage):
    def __init__(self, previous_block: Sha256Hash, block_hash: Sha256Hash):
        self.previous_block = previous_block
        self.block_hash = block_hash
        self._rawbytes = memoryview(
            self.previous_block.binary + self.block_hash.binary
        )

    def __eq__(self, other):
        return (
            isinstance(other, TestBlockMessage)
            and other.block_hash == self.block_hash
        )

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
    def __init__(self, node):
        super().__init__(node)
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


def create_block_message(
    block_hash: Optional[Sha256Hash] = None,
    previous_block_hash: Optional[Sha256Hash] = None,
) -> TestBlockMessage:
    if block_hash is None:
        block_hash = Sha256Hash(helpers.generate_hash())
    if previous_block_hash is None:
        previous_block_hash = Sha256Hash(helpers.generate_hash())
    return TestBlockMessage(previous_block_hash, block_hash)


class BlockQueuingServiceTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(
            helpers.get_gateway_opts(
                8000,
                max_block_interval=gateway_constants.MAX_INTERVAL_BETWEEN_BLOCKS_S,
                blockchain_message_ttl=300
            )
        )

        self.node_connection = Mock()
        self.node_connection.is_active = MagicMock(return_value=True)

        self.node.node_conn = self.node_connection

        self.block_queuing_service = TestPushBlockQueuingService(self.node)

    def test_block_added_to_empty_queue(self):
        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = create_block_message(block_hash)

        self.block_queuing_service.push(
            block_hash, block_msg, waiting_for_recovery=False
        )

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg, self.node.send_to_node_messages[0])

        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(1, len(self.block_queuing_service.blocks_sent))
        self.assertEqual(block_hash, self.block_queuing_service.blocks_sent[0])

        # block still in service after confirmation
        self.assertIn(block_hash, self.block_queuing_service)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash
        )
        self.assertIn(block_hash, self.block_queuing_service)

    def test_block_added_when_node_is_not_ready(self):
        self.node.node_conn = None

        block_hash = Sha256Hash(helpers.generate_hash())
        block_msg = create_block_message(block_hash)

        self.block_queuing_service.push(
            block_hash, block_msg, waiting_for_recovery=False
        )

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertIn(block_hash, self.block_queuing_service)

        time.time = MagicMock(
            return_value=time.time()
            + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S
        )
        self.node.alarm_queue.fire_alarms()

        # verify that item is still in the queue if node connection
        # is still not available
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertIn(block_hash, self.block_queuing_service)

        self.node.node_conn = Mock()
        self.node.node_conn.is_active = MagicMock(return_value=False)

        time.time = MagicMock(
            return_value=time.time()
            + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S * 2
        )
        self.node.alarm_queue.fire_alarms()

        # verify that item is still in the queue if node connection
        # is available but not active
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertIn(block_hash, self.block_queuing_service)

        self.node.node_conn.is_active = MagicMock(return_value=True)

        time.time = MagicMock(
            return_value=time.time()
            + NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S * 3
        )
        self.node.alarm_queue.fire_alarms()

        # verify that is sent to node once connection to node is active
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))

        # verify item still cached, since confirmation not received
        self.assertIn(block_hash, self.block_queuing_service)

    def test_block_added_waiting_for_previous_block_confirmation(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.block_queuing_service.push(block_hash_1, block_msg_1)
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_1, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)

        # no confirmation, wait on block 1
        self.block_queuing_service.push(block_hash_2, block_msg_2)
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_1, self.node.send_to_node_messages[0])
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)
        self.assertIn(block_hash_2, self.block_queuing_service)

        self.block_queuing_service.mark_blocks_seen_by_blockchain_node(
            [block_hash_1]
        )
        self.assertEqual(2, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_2, self.node.send_to_node_messages[1])
        self.assertEqual(0, len(self.block_queuing_service))

    def test_block_added_send_immediately_with_confirmation(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.block_queuing_service.push(block_hash_1, block_msg_1)
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_1, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)

        # confirmed, send immediately
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1
        )
        self.block_queuing_service.push(block_hash_2, block_msg_2)
        self.assertEqual(2, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_2, self.node.send_to_node_messages[1])
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)
        self.assertIn(block_hash_2, self.block_queuing_service)

    def test_waiting_confirmation_timeout(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.block_queuing_service.push(block_hash_1, block_msg_1)
        self.block_queuing_service.push(block_hash_2, block_msg_2)

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_1, self.node.send_to_node_messages[0])
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)
        self.assertIn(block_hash_2, self.block_queuing_service)

        time.time = MagicMock(
            return_value=time.time()
            + gateway_constants.MAX_INTERVAL_BETWEEN_BLOCKS_S
        )
        self.node.alarm_queue.fire_alarms()

        self.assertEqual(2, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_2, self.node.send_to_node_messages[1])
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)
        self.assertIn(block_hash_2, self.block_queuing_service)

    def test_waiting_recovery_block_to_empty_queue(self):
        block_hash = Sha256Hash(helpers.generate_hash())

        self.block_queuing_service.push(
            block_hash, None, waiting_for_recovery=True
        )
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertIn(block_hash, self.block_queuing_service)

    def test_waiting_recovery__timeout(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = create_block_message(block_hash2)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = create_block_message(block_hash3)

        self.block_queuing_service.push(
            block_hash1, None, waiting_for_recovery=True
        )
        self.block_queuing_service.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.block_queuing_service.push(
            block_hash3, block_msg3, waiting_for_recovery=False
        )

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))
        self.assertIn(block_hash1, self.block_queuing_service)
        self.assertIn(block_hash2, self.block_queuing_service)
        self.assertIn(block_hash3, self.block_queuing_service)

        # fire alarm when block recovery not expired
        time.time = MagicMock(
            return_value=time.time() + BLOCK_RECOVERY_MAX_QUEUE_TIME / 2 + 1
        )
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))
        self.assertIn(block_hash1, self.block_queuing_service)
        self.assertIn(block_hash2, self.block_queuing_service)
        self.assertIn(block_hash3, self.block_queuing_service)

        # fire alarm after recovery expires and is not successful
        time.time = MagicMock(
            return_value=time.time() + BLOCK_RECOVERY_MAX_QUEUE_TIME
        )
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])
        self.assertNotIn(block_hash1, self.block_queuing_service)
        self.assertIn(block_hash2, self.block_queuing_service)
        self.assertIn(block_hash3, self.block_queuing_service)

    def test_last_block_is_recovered(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())
        block_msg1 = create_block_message()

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = create_block_message(block_hash3, block_hash2)

        self.block_queuing_service.push(
            block_hash1, block_msg1, waiting_for_recovery=False
        )
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash1
        )
        self.block_queuing_service.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash2
        )
        self.block_queuing_service.push(
            block_hash3, block_msg3, waiting_for_recovery=True
        )

        self.assertEqual(2, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg1, self.node.send_to_node_messages[0])
        self.assertEqual(block_msg2, self.node.send_to_node_messages[1])

        self.node.send_to_node_messages = []

        # don't send, since not recovered enough though timeout
        time.time = MagicMock(
            return_value=time.time() + MAX_INTERVAL_BETWEEN_BLOCKS_S
        )
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        self.block_queuing_service.update_recovered_block(
            block_hash3, block_msg3
        )
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg3, self.node.send_to_node_messages[0])

    def test_top_block_is_recovered(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())
        block_msg1 = create_block_message(block_hash1)

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = create_block_message(block_hash3, block_hash2)

        self.block_queuing_service.push(
            block_hash1, None, waiting_for_recovery=True
        )
        self.block_queuing_service.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.block_queuing_service.push(
            block_hash3, block_msg3, waiting_for_recovery=False
        )

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))

        self.block_queuing_service.update_recovered_block(
            block_hash1, block_msg1
        )
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))
        self.assertEqual(block_msg1, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))

        self.block_queuing_service.mark_blocks_seen_by_blockchain_node(
            [block_hash1]
        )
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        self.block_queuing_service.mark_blocks_seen_by_blockchain_node(
            [block_hash2]
        )
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg3, self.node.send_to_node_messages[0])

    def test_waiting_recovery_top_block_timeout(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = create_block_message(block_hash3, block_hash2)

        self.block_queuing_service.push(
            block_hash1, None, waiting_for_recovery=True
        )
        self.block_queuing_service.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.block_queuing_service.push(
            block_hash3, block_msg3, waiting_for_recovery=False
        )

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(3, len(self.block_queuing_service))
        self.assertIn(block_hash1, self.block_queuing_service)
        self.assertIn(block_hash2, self.block_queuing_service)
        self.assertIn(block_hash3, self.block_queuing_service)

        # trigger automatic removal of top block recovery
        time.time = MagicMock(
            return_value=time.time()
            + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME
        )
        self.node.alarm_queue.fire_alarms()

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]
        self.block_queuing_service.mark_blocks_seen_by_blockchain_node(
            [block_hash2]
        )

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg3, self.node.send_to_node_messages[0])

    def test_waiting_recovery_last_block_is_removed(self):
        block_hash1 = Sha256Hash(helpers.generate_hash())
        block_msg1 = create_block_message(block_hash1)

        block_hash2 = Sha256Hash(helpers.generate_hash())
        block_msg2 = create_block_message(block_hash2, block_hash1)

        block_hash3 = Sha256Hash(helpers.generate_hash())
        block_msg3 = create_block_message(block_hash3, block_hash2)

        self.block_queuing_service.push(
            block_hash1, block_msg1, waiting_for_recovery=False
        )
        self.block_queuing_service.push(
            block_hash2, block_msg2, waiting_for_recovery=False
        )
        self.block_queuing_service.push(
            block_hash3, block_msg3, waiting_for_recovery=True
        )

        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))
        self.assertEqual(block_msg1, self.node.send_to_node_messages[0])

        del self.node.send_to_node_messages[0]

        self.block_queuing_service.remove(block_hash3)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(1, len(self.block_queuing_service))

        self.block_queuing_service.mark_blocks_seen_by_blockchain_node(
            [block_hash1]
        )
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertEqual(block_msg2, self.node.send_to_node_messages[0])

    def test_sending_headers_to_node(self):
        block_hash = Sha256Hash(helpers.generate_hash())
        block_message = create_block_message(block_hash)
        self.block_queuing_service.push(block_hash, block_message)
        success = self.block_queuing_service.try_send_header_to_node(block_hash)
        self.assertTrue(success)
        self.assertEqual(2, len(self.node.send_to_node_messages))

        block_header_message = self.node.send_to_node_messages[1]
        self.assertIsInstance(block_header_message, TestBlockHeaderMessage)
        self.assertEqual(block_hash, block_header_message.block_hash)

    def test_sending_headers_to_node_recovery(self):
        block_hash = Sha256Hash(helpers.generate_hash())
        self.block_queuing_service.push(
            block_hash, None, waiting_for_recovery=True
        )
        success = self.block_queuing_service.try_send_header_to_node(block_hash)
        self.assertFalse(success)
        self.assertEqual(0, len(self.node.send_to_node_messages))

    def test_sending_block_timed_out(self):
        block_hash_1 = Sha256Hash(helpers.generate_hash())
        block_msg_1 = create_block_message(block_hash_1)

        block_hash_2 = Sha256Hash(helpers.generate_hash())
        block_msg_2 = create_block_message(block_hash_2, block_hash_1)

        # first block gets sent immediately
        self.block_queuing_service.push(block_hash_1, block_msg_1)
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_1, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))
        self.assertIn(block_hash_1, self.block_queuing_service)

        self.block_queuing_service.push(block_hash_2, block_msg_2)

        time.time = MagicMock(return_value=time.time() + 350)

        # too much time has elapsed, message is dropped
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        self.assertEqual(block_msg_1, self.node.send_to_node_messages[0])
        self.assertEqual(0, len(self.block_queuing_service))
