import time
from unittest import TestCase

from mock import MagicMock

from bxcommon.messages.bloxroute.ping_message import PingMessage
from bxgateway.utils.blockchain_message_queue import BlockchainMessageQueue

TTL = 10


class BlockchainMessageQueueTest(TestCase):

    def setUp(self) -> None:
        self.blockchain_message_queue = BlockchainMessageQueue(TTL)

    def test_append(self):
        message_1 = PingMessage(1)
        message_2 = PingMessage(2)
        message_3 = PingMessage(3)

        self.blockchain_message_queue.append(message_1)
        self.assertIn(message_1, self.blockchain_message_queue._queue)

        self.blockchain_message_queue.append(message_2)
        self.assertIn(message_2, self.blockchain_message_queue._queue)

        time.time = MagicMock(return_value=time.time() + TTL + 1)

        # appends after timeout should be ignored
        self.blockchain_message_queue.append(message_3)
        self.assertNotIn(message_3, self.blockchain_message_queue._queue)

    def test_get_and_clear_before_timeout(self):
        message_1 = PingMessage(1)
        message_2 = PingMessage(2)

        self.blockchain_message_queue.append(message_1)
        self.assertIn(message_1, self.blockchain_message_queue._queue)

        self.blockchain_message_queue.append(message_2)
        self.assertIn(message_2, self.blockchain_message_queue._queue)

        items = self.blockchain_message_queue.pop_items()
        self.assertEqual(2, len(items))
        self.assertIn(message_1, items)
        self.assertIn(message_2, items)

        next_items = self.blockchain_message_queue.pop_items()
        self.assertEqual(0, len(next_items))

        message_3 = PingMessage(3)
        self.blockchain_message_queue.append(message_3)

        last_items = self.blockchain_message_queue.pop_items()
        self.assertEqual(1, len(last_items))
        self.assertIn(message_3, last_items)

    def test_get_and_clear_after_timeout(self):
        message_1 = PingMessage(1)
        message_2 = PingMessage(2)

        self.blockchain_message_queue.append(message_1)
        self.assertIn(message_1, self.blockchain_message_queue._queue)

        self.blockchain_message_queue.append(message_2)
        self.assertIn(message_2, self.blockchain_message_queue._queue)

        time.time = MagicMock(return_value=time.time() + TTL + 1)

        items = self.blockchain_message_queue.pop_items()
        self.assertEqual(0, len(items))

    def test_get_and_clear_reenables_insert(self):
        message_1 = PingMessage(1)
        message_2 = PingMessage(2)
        message_3 = PingMessage(3)

        self.blockchain_message_queue.append(message_1)
        self.assertIn(message_1, self.blockchain_message_queue._queue)

        time.time = MagicMock(return_value=time.time() + TTL + 1)

        # appends after timeout should be ignored
        self.blockchain_message_queue.append(message_2)
        self.assertNotIn(message_2, self.blockchain_message_queue._queue)

        items = self.blockchain_message_queue.pop_items()
        self.assertEqual(0, len(items))

        self.blockchain_message_queue.append(message_3)
        self.assertIn(message_3, self.blockchain_message_queue._queue)
