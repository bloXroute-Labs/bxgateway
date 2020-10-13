import os

from bxgateway.testing import gateway_helpers
from bxcommon.utils import convert

from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.services.ont.ont_block_queuing_service import OntBlockQueuingService
from mock import MagicMock, Mock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class OntBlockQueuingServiceTest(AbstractTestCase):
    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000, max_block_interval_s=0)
        )

        self.node_connection = Mock()
        self.node_connection.is_active = MagicMock(return_value=True)
        self.node.set_known_total_difficulty = MagicMock()

        self.node.node_conn = self.node_connection

        self.block_queuing_service = OntBlockQueuingService(self.node)

        self.block_hashes = []
        self.block_msg = self._get_sample_block()
        self.block_queuing_service.store_block_data(self.block_msg.block_hash(), self.block_msg)

    def test_get_recent_blocks(self):
        recent_blocks = list(self.block_queuing_service.iterate_recent_block_hashes(10))
        self.assertEqual(len(recent_blocks), 1)
        self.assertEqual(recent_blocks[0], self.block_msg.block_hash())

    def _get_sample_block(self, file_path=__file__):
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(file_path))))
        with open(os.path.join(root_dir, "samples/ont_sample_block.txt")) as sample_file:
            ont_block = sample_file.read().strip("\n")
        buf = bytearray(convert.hex_to_bytes(ont_block))
        parsed_block = BlockOntMessage(buf=buf)
        return parsed_block

    def test_tracked_block_cleanup(self):
        self.node.block_queuing_service = self.block_queuing_service
        tx_service = self.node.get_tx_service()
        for block_hash in self.block_queuing_service.iterate_recent_block_hashes(10):
            tx_service.track_seen_short_ids(block_hash, [])
        self.node._tracked_block_cleanup()
