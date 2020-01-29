from mock import MagicMock, Mock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils import helpers

from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.services.eth.eth_block_queuing_service import (
    EthBlockQueuingService,
)
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from mock import MagicMock, Mock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.services.eth.eth_block_queuing_service import (
    EthBlockQueuingService,
)
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class EthBlockQueuingServiceTest(AbstractTestCase):
    def setUp(self):
        self.node = MockGatewayNode(
            helpers.get_gateway_opts(8000, max_block_interval=0)
        )

        self.node_connection = Mock()
        self.node_connection.is_active = MagicMock(return_value=True)
        self.node.set_known_total_difficulty = MagicMock()

        self.node.node_conn = self.node_connection

        self.block_queuing_service = EthBlockQueuingService(self.node)

        self.block_hashes = []

        # block numbers: 1000-1019
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000)
            )
            block_hash = block_message.block_hash()
            self.block_hashes.append(block_hash)
            self.block_queuing_service.push(block_hash, block_message)

    def test_get_block_hashes_from_hash(self):
        # request: start: 1019, count: 1
        # result: 1019
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[19], 1, 0, False
        )
        self.assertEqual(1, len(hashes))
        self.assertEqual(self.block_hashes[19], hashes[0])

        # request: start: 1010, count: 10
        # result: 1010, 1011, .. 1019
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[10], 10, 0, False
        )
        self.assertEqual(10, len(hashes))
        for i, block_hash in enumerate(hashes):
            self.assertEqual(self.block_hashes[i + 10], hashes[i])

        # request: start: 1010, count: 2, skip: 5
        # result: 1010, 1016
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[10], 2, 5, False
        )
        self.assertEqual(2, len(hashes))
        self.assertEqual(self.block_hashes[10], hashes[0])
        self.assertEqual(self.block_hashes[16], hashes[1])

        # request: start: 1010, count: 2, skip: 5, reverse = True
        # result: 1010, 1004
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[10], 2, 5, True
        )
        self.assertEqual(2, len(hashes))
        self.assertEqual(self.block_hashes[10], hashes[0])
        self.assertEqual(self.block_hashes[4], hashes[1])

    def test_get_block_hashes_from_height(self):
        hashes = self.block_queuing_service.get_block_hashes_starting_from_height(
            1019, 1, 0, False
        )
        self.assertEqual(1, len(hashes))
        self.assertEqual(self.block_hashes[19], hashes[0])

    def test_get_block_hashes_fork_aborts(self):
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[10], 5, 0, False
        )
        self.assertEqual(5, len(hashes))

        # create fork at block 17
        block_message = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(21, 1017)
        )
        block_hash = block_message.block_hash()
        self.block_queuing_service.push(block_hash, block_message)
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[10], 10, 0, False
        )
        self.assertEqual(0, len(hashes))

    def test_update_recovered_block(self):
        self.block_queuing_service = EthBlockQueuingService(self.node)
        self.node.send_to_node_messages.clear()

        block_message_1 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(1, 1)
        )
        block_hash_1 = block_message_1.block_hash()
        block_message_2 = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(2, 2)
        )
        block_hash_2 = block_message_2.block_hash()

        self.block_queuing_service.push(block_hash_1, waiting_for_recovery=True)
        self.block_queuing_service.push(block_hash_2, block_message_2)

        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.assertEqual(2, len(self.block_queuing_service))

        self.block_queuing_service.update_recovered_block(
            block_hash_1,
            block_message_1
        )
        self.assertEqual(2, len(self.node.send_to_node_messages))
        self.assertEqual(0, len(self.block_queuing_service))

