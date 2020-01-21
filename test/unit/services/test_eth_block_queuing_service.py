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
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000)
            )
            block_hash = block_message.block_hash()
            self.block_hashes.append(block_hash)
            self.block_queuing_service.push(block_hash, block_message)

    def test_get_block_hashes_from_hash(self):
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[-1], 1, 0, False
        )
        self.assertEqual(1, len(hashes))
        self.assertEqual(self.block_hashes[-1], hashes[0])

        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[-1], 10, 0, False
        )

        self.assertEqual(10, len(hashes))
        for i, block_hash in enumerate(hashes):
            self.assertEqual(self.block_hashes[-1 - i], hashes[i])

        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[-1], 2, 5, False
        )
        self.assertEqual(2, len(hashes))
        self.assertEqual(self.block_hashes[-1], hashes[0])
        self.assertEqual(self.block_hashes[-7], hashes[1])

        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[-1], 2, 5, True
        )
        self.assertEqual(2, len(hashes))
        self.assertEqual(self.block_hashes[-1], hashes[1])
        self.assertEqual(self.block_hashes[-7], hashes[0])

    def test_get_block_hashes_from_height(self):
        hashes = self.block_queuing_service.get_block_hashes_starting_from_height(
            1019, 1, 0, False
        )
        self.assertEqual(1, len(hashes))
        self.assertEqual(self.block_hashes[-1], hashes[0])

    def test_get_block_hashes_fork_aborts(self):
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[-1], 5, 0, False
        )
        self.assertEqual(5, len(hashes))

        # create fork at block 17
        block_message = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(21, 1017)
        )
        block_hash = block_message.block_hash()
        self.block_queuing_service.push(block_hash, block_message)
        hashes = self.block_queuing_service.get_block_hashes_starting_from_hash(
            self.block_hashes[-1], 5, 0, False
        )
        self.assertEqual(0, len(hashes))