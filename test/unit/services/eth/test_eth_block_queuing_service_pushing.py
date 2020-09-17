import struct
import time
from collections import deque
from typing import List
from unittest.mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.object_hash import Sha256Hash, NULL_SHA256_HASH
from bxgateway import gateway_constants
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import \
    BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import \
    GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import \
    NewBlockEthProtocolMessage
from bxgateway.services.eth.eth_block_processing_service import EthBlockProcessingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService, EthBlockInfo
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode

BLOCK_INTERVAL = 0.1


class EthBlockQueuingServicePushingTest(AbstractTestCase):
    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000, max_block_interval_s=BLOCK_INTERVAL)
        )
        self.node.alarm_queue = AlarmQueue()

        self.node_connection = MagicMock()
        self.node_connection.is_active = MagicMock(return_value=True)
        self.node.set_known_total_difficulty = MagicMock()

        self.node.node_conn = self.node_connection

        self.block_queuing_service = EthBlockQueuingService(self.node)
        self.block_processing_service = EthBlockProcessingService(self.node)

        self.node.block_queuing_service = self.block_queuing_service
        self.node.block_processing_service = self.block_processing_service

        self.node.broadcast = MagicMock()

        time.time = MagicMock(return_value=time.time())

    def test_accepted_blocks_pushes_next_immediately(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self.block_queuing_service.push(block_hash_2, block_2)
        self.block_queuing_service.push(block_hash_3, block_3)
        self._assert_block_sent(block_hash_1)

        # after block 1 is accepted, block 2 is immediately pushed
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )
        self._assert_block_sent(block_hash_2)

        self._progress_time()
        self._assert_block_sent(block_hash_3)

    # receive 1, 2a, 2b, 3b, 4b
    # confirm 1, 2a
    # send 1 (instant), 2a (instant), 3b (instant), 4b (after delay)
    def test_handle_single_block_fork_already_accepted(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2a = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2a = block_2a.block_hash()
        block_2b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 2, block_hash_1)
        )
        block_hash_2b = block_2b.block_hash()

        block_3b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(4, 3, block_hash_2b)
        )
        block_hash_3b = block_3b.block_hash()
        block_4b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(5, 4, block_hash_3b)
        )
        block_hash_4b = block_4b.block_hash()

        # accept block 1
        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )

        # accept block 2a
        self.block_queuing_service.push(block_hash_2a, block_2a)
        self._assert_block_sent(block_hash_2a)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_2a, block_2a
        )

        # block 2b will never be sent
        self.block_queuing_service.push(block_hash_2b, block_2b)
        self._assert_no_blocks_sent()

        # block 3b will be sent
        self.block_queuing_service.push(block_hash_3b, block_3b)
        self._assert_block_sent(block_hash_3b)

        # sync triggered, requesting 2a by hash
        self.block_processing_service.try_process_get_block_headers_request(
            GetBlockHeadersEthProtocolMessage(
                None, block_hash_2a.binary, 1, 0, 0
            )
        )
        # response is empty
        self._assert_headers_sent([])

        # request block 1 to establish common ancestor
        block_number_bytes = struct.pack(">I", 1)
        self.block_processing_service.try_process_get_block_headers_request(
            GetBlockHeadersEthProtocolMessage(
                None, block_number_bytes, 1, 0, 0
            )
        )
        self._assert_headers_sent([block_hash_1])

        # request block 193, 193 + 191, etc to determine chain state
        block_number_bytes = struct.pack(">I", 193)
        self.block_processing_service.try_process_get_block_headers_request(
            GetBlockHeadersEthProtocolMessage(
                None, block_number_bytes, 128, 191, 0
            )
        )
        self._assert_headers_sent([])

        # request block 2, 3, 4, ... to compare state
        block_number_bytes = struct.pack(">I", 2)
        self.block_processing_service.try_process_get_block_headers_request(
            GetBlockHeadersEthProtocolMessage(
                None, block_number_bytes, 192, 0, 0
            )
        )
        self._assert_headers_sent([block_hash_2b, block_hash_3b])

        # request block 4, 5, 6, ... to compare state
        block_number_bytes = struct.pack(">I", 4)
        self.block_processing_service.try_process_get_block_headers_request(
            GetBlockHeadersEthProtocolMessage(
                None, block_number_bytes, 192, 0, 0
            )
        )
        self._assert_headers_sent([])

        # 4b is sent after timeout (presumably Ethereum node didn't send back acceptance
        # because it's resolving chainstate)
        self.block_queuing_service.push(block_hash_4b, block_4b)
        self._assert_no_blocks_sent()

        self._progress_time()
        self._assert_block_sent(block_hash_4b)

    def test_handle_single_block_fork_accepted_later(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2a = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2a = block_2a.block_hash()
        block_2b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 2, block_hash_1)
        )
        block_hash_2b = block_2b.block_hash()

        block_3b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(4, 3, block_hash_2b)
        )
        block_hash_3b = block_3b.block_hash()
        block_4b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(5, 4, block_hash_3b)
        )
        block_hash_4b = block_4b.block_hash()

        # send + accept block 1
        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )

        # send block 2a
        self.block_queuing_service.push(block_hash_2a, block_2a)
        self._assert_block_sent(block_hash_2a)

        # block 2b will never be sent (despite no confirmation)
        self.block_queuing_service.push(block_hash_2b, block_2b)
        self._assert_no_blocks_sent()

        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_2a, block_2a
        )

        # block 3b will be sent immediately (block at height 2 confirmed)
        self.block_queuing_service.push(block_hash_3b, block_3b)
        self._assert_block_sent(block_hash_3b)

        # block 4b send later (no confirmation for height 3)
        self.block_queuing_service.push(block_hash_4b, block_4b)
        self._assert_no_blocks_sent()

        self._progress_time()
        self._assert_block_sent(block_hash_4b)

    def test_handle_out_of_order_blocks(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()

        # second block won't be sent, was somehow not received from BDN
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        # third block should still be queued and sent after timeout
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )

        # block 3 sent after a delay, since no block 2 was ever seen
        self.block_queuing_service.push(block_hash_3, block_3)
        self._assert_no_blocks_sent()

        self._progress_time()
        self._assert_block_sent(block_hash_3)

    def test_handle_out_of_order_blocks_confirmed_ancestor(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()

        # block 2 will be received after block 3, but still should be pushed to
        # blockchain node first since block 1 is confirmed and timeout not reached
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )

        self.block_queuing_service.push(block_hash_3, block_3)
        self._assert_no_blocks_sent()

        self.block_queuing_service.push(block_hash_2, block_2)
        self._assert_block_sent(block_hash_2)

        self._progress_time()
        self._assert_block_sent(block_hash_3)

    def test_handle_out_of_order_blocks_unconfirmed_ancestor(self):
        self._set_block_queue_in_progress()

        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        # block 2 will be received after block 3, but still should be pushed to
        # blockchain node first after timeout (no confirmation on block 1)
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        # after another timeout, block 3 will be pushed
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)

        self.block_queuing_service.push(block_hash_3, block_3)
        self._assert_no_blocks_sent()

        self.block_queuing_service.push(block_hash_2, block_2)
        self._assert_no_blocks_sent()

        self._progress_time()
        self._assert_block_sent(block_hash_2)

        self._progress_time()
        self._assert_block_sent(block_hash_3)

    def test_handle_recovering_block_timeout(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )

        self.block_queuing_service.push(block_hash_2, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash_3, block_3)

        # sent block 3 anyway, block 2 is taking too long to recover
        self._progress_time()
        self._assert_block_sent(block_hash_3)

        # recovery times out in the end, nothing sent and queue cleared
        self._progress_recovery_timeout()
        self._assert_no_blocks_sent()
        self.assertEqual(0, len(self.block_queuing_service))

    def test_handle_recovering_block_recovered_quickly(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_1, block_1
        )

        self.block_queuing_service.push(block_hash_2, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash_3, block_3)

        self._assert_no_blocks_sent()

        # block 2 recovers quickly enough, is sent ahead of block 3
        self.block_queuing_service.update_recovered_block(block_hash_2, block_2)
        self._assert_block_sent(block_hash_2)

        self._progress_time()
        self._assert_block_sent(block_hash_3)

    def test_handle_recovering_block_recovered_ordering(self):
        self._set_block_queue_in_progress()

        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        # blocks in recovery get added out of order
        self.block_queuing_service.push(block_hash_2, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash_1, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash_3, block_3)

        self._assert_no_blocks_sent()

        # everything in recovery recovers quickly, so correct order re-established
        self.block_queuing_service.update_recovered_block(block_hash_2, block_2)
        self.block_queuing_service.update_recovered_block(block_hash_1, block_1)
        self._assert_block_sent(block_hash_1)

        self._progress_time()
        self._assert_block_sent(block_hash_2)

        self._progress_time()
        self._assert_block_sent(block_hash_3)

    def test_handle_recovering_block_recovered_too_slow(self):
        self._set_block_queue_in_progress()

        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        self.block_queuing_service.push(block_hash_1, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash_2, block_2)

        self._assert_no_blocks_sent()

        # block 1 takes too long to recover, so block 2 is just sent
        self._progress_time()
        self._assert_block_sent(block_hash_2)

        # block 1 is now stale, queue should be empty
        self.block_queuing_service.update_recovered_block(block_hash_1, block_1)
        self._assert_no_blocks_sent()

        self._progress_time()
        self._assert_no_blocks_sent()

    def test_dont_send_stale_blocks_newer_block_confirmed(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_2, block_2
        )

        # block 1 will be ignored by queue, since block 2 already confirmed
        self.block_queuing_service.push(block_hash_1, block_1)

        self._assert_no_blocks_sent()
        self._progress_time()
        self._assert_no_blocks_sent()

        self.assertEqual(0, len(self.block_queuing_service))

    def test_dont_send_stale_blocks_newer_block_sent(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        self.block_queuing_service.push(block_hash_2, block_2)
        self._assert_block_sent(block_hash_2)

        # block 1 will be ignored by queue, since block 2 already sent
        self.block_queuing_service.push(block_hash_1, block_1)

        self._assert_no_blocks_sent()
        self._progress_time()
        self._assert_no_blocks_sent()

        self.assertEqual(0, len(self.block_queuing_service))

    def test_dont_send_stale_recovered_block(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_2, block_2
        )
        self.block_queuing_service.push(block_hash_1, waiting_for_recovery=True)

        # block 1 will be ignored by queue after recovery (didn't know block
        # number before then), since block 2 already confirmed
        self.block_queuing_service.update_recovered_block(
            block_hash_1, block_1
        )

        self._assert_no_blocks_sent()
        self._progress_time()
        self._assert_no_blocks_sent()

        self.assertEqual(0, len(self.block_queuing_service))

    def test_dont_send_seen_block(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self.block_queuing_service.push(block_hash_2, block_2)

        self._assert_block_sent(block_hash_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash_2, block_2
        )

        # block 2 marked seen by blockchain node, so eject block 2
        self._progress_time()
        self._assert_no_blocks_sent()

        self.assertEqual(0, len(self.block_queuing_service))

    def test_clear_out_stale_blocks(self):
        self._set_block_queue_in_progress()

        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_2, block_2)
        self.block_queuing_service.push(block_hash_3, block_3)
        self._assert_no_blocks_sent()

        # block 3 marked seen by blockchain node, so eject all earlier blocks (2, 3)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_3, block_3)
        self._assert_no_blocks_sent()

        self.assertEqual(0, len(self.block_queuing_service))

    def test_partial_chainstate(self):
        self.assertEqual(deque(), self.block_queuing_service.partial_chainstate(10))

        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_1, block_1)
        self.block_queuing_service.push(block_hash_2, block_2)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_2, block_2)
        self.block_queuing_service.push(block_hash_3, block_3)

        self.assertEqual(0, len(self.block_queuing_service))
        expected_result = deque(
            [
                EthBlockInfo(1, block_hash_1),
                EthBlockInfo(2, block_hash_2),
                EthBlockInfo(3, block_hash_3),
            ]
        )
        self.assertEqual(expected_result, self.block_queuing_service.partial_chainstate(3))

        # returns full chain if requested length is shorter
        self.assertEqual(expected_result, self.block_queuing_service.partial_chainstate(1))

        # not more data, so can't meet asked length
        next_expected_result = deque(
            [
                EthBlockInfo(1, block_hash_1),
                EthBlockInfo(2, block_hash_2),
                EthBlockInfo(3, block_hash_3),
            ]
        )
        self.assertEqual(next_expected_result, self.block_queuing_service.partial_chainstate(10))

    def test_partial_chainstate_reorganizes(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_2b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 2, block_hash_1)
        )
        block_hash_2b = block_2b.block_hash()
        block_3b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(4, 3, block_hash_2b)
        )
        block_hash_3b = block_3b.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_1, block_1)
        self.block_queuing_service.push(block_hash_2, block_2)
        self.block_queuing_service.push(block_hash_2b, block_2b)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_2, block_2)

        expected_result = deque(
            [
                EthBlockInfo(1, block_hash_1),
                EthBlockInfo(2, block_hash_2),
            ]
        )
        self.assertEqual(expected_result, self.block_queuing_service.partial_chainstate(10))

        self.block_queuing_service.push(block_hash_3b, block_3b)

        next_expected_result = deque(
            [
                EthBlockInfo(1, block_hash_1),
                EthBlockInfo(2, block_hash_2b),
                EthBlockInfo(3, block_hash_3b),
            ]
        )
        self.assertEqual(next_expected_result, self.block_queuing_service.partial_chainstate(10))

    def test_partial_chainstate_missing_entries(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        # 2b will never be pushed to block queue
        block_2b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 2, block_hash_1)
        )
        block_hash_2b = block_2b.block_hash()
        block_3b = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(4, 3, block_hash_2b)
        )
        block_hash_3b = block_3b.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_1, block_1)
        self.block_queuing_service.push(block_hash_2, block_2)

        # establish an earlier chainstate first, just for testing
        self.block_queuing_service.partial_chainstate(10)

        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_2, block_2)
        self.block_queuing_service.push(block_hash_3b, block_3b)

        expected_result = deque(
            [
                EthBlockInfo(3, block_hash_3b),
            ]
        )
        self.assertEqual(expected_result, self.block_queuing_service.partial_chainstate(10))

    def test_partial_chainstate_missing_entries_in_head(self):
        block_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_1.block_hash()
        block_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2, block_hash_1)
        )
        block_hash_2 = block_2.block_hash()
        block_3 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(3, 3, block_hash_2)
        )
        block_hash_3 = block_3.block_hash()

        self.block_queuing_service.push(block_hash_1, block_1)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash_1, block_1)

        # establish an earlier chainstate first, just for extending later
        self.block_queuing_service.partial_chainstate(10)

        self.block_queuing_service.push(block_hash_3, block_3)
        self._progress_time()

        expected_result = deque(
            [
                EthBlockInfo(3, block_hash_3),
            ]
        )
        self.assertEqual(expected_result, self.block_queuing_service.partial_chainstate(10))

    def _assert_block_sent(self, block_hash: Sha256Hash) -> None:
        self.node.broadcast.assert_called()

        num_calls = 0
        for call_args in self.node.broadcast.call_args_list:
            ((block_message, ), _) = call_args
            if isinstance(block_message, NewBlockEthProtocolMessage):
                self.assertEqual(block_hash, block_message.block_hash())
                num_calls += 1

        self.assertEqual(1, num_calls, "No blocks were sent")
        self.node.broadcast.reset_mock()

    def _assert_no_blocks_sent(self) -> None:
        for call_args in self.node.broadcast.call_args_list:
            ((block_message, ), _) = call_args
            if isinstance(block_message, NewBlockEthProtocolMessage):
                self.fail(f"Unexpected block sent: {block_message.block_hash()}")

    def _assert_headers_sent(self, hashes: List[Sha256Hash]):
        self.node.broadcast.assert_called_once()
        ((headers_message, ), _) = self.node.broadcast.call_args
        assert isinstance(headers_message, BlockHeadersEthProtocolMessage)

        headers = headers_message.get_block_headers()
        self.assertEqual(len(hashes), len(headers))

        for expected_hash, header in zip(hashes, headers):
            self.assertEqual(expected_hash, header.hash_object())

        self.node.broadcast.reset_mock()

    def _progress_time(self):
        time.time = MagicMock(return_value=time.time() + BLOCK_INTERVAL)
        self.node.alarm_queue.fire_alarms()

    def _progress_recovery_timeout(self):
        time.time = MagicMock(
            return_value=time.time() + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME
        )
        self.node.alarm_queue.fire_alarms()

    def _set_block_queue_in_progress(self):
        # pretend block queuing service is in progress
        self.block_queuing_service.best_sent_block = (0, NULL_SHA256_HASH, time.time())
        self.block_queuing_service.best_accepted_block = (0, NULL_SHA256_HASH)

