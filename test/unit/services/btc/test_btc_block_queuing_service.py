import time
import os

from bxgateway.testing import gateway_helpers
from bxcommon.utils import convert

from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.services.btc.btc_block_queuing_service import BtcBlockQueuingService
from mock import MagicMock, Mock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.services.btc.btc_normal_block_cleanup_service import BtcNormalBlockCleanupService
from bxcommon import constants


class BtcBlockQueuingServiceTest(AbstractTestCase):
    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000, max_block_interval_s=0)
        )

        self.node_connection = Mock()
        self.node_connection.is_active = MagicMock(return_value=True)
        self.node.set_known_total_difficulty = MagicMock()

        self.node.node_conn = self.node_connection

        self.block_queuing_service = BtcBlockQueuingService(self.node)
        self.node.block_queuing_service = self.block_queuing_service
        self.node.block_cleanup_service = BtcNormalBlockCleanupService(self.node, 1)
        self.block_hashes = []
        self.block_msg = self._get_sample_block()
        self.block_queuing_service.store_block_data(self.block_msg.block_hash(), self.block_msg)
        self.node.get_tx_service().track_seen_short_ids(self.block_msg.block_hash(), [])

    def _get_sample_block(self, file_path=__file__):
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(file_path))))
        with open(os.path.join(root_dir, "samples/btc_sample_block.txt")) as sample_file:
            btc_block = sample_file.read().strip("\n")
        buf = bytearray(convert.hex_to_bytes(btc_block))
        parsed_block = BlockBtcMessage(buf=buf)
        return parsed_block

    def test_tracked_block_cleanup(self):
        tx_service = self.node.get_tx_service()
        self.assertEqual(1, len(tx_service.get_tracked_blocks()))
        self.node.block_cleanup_service.block_cleanup_request(self.block_msg.block_hash())
        time.time = MagicMock(return_value=time.time() + constants.MIN_SLEEP_TIMEOUT)
        self.node.alarm_queue.fire_alarms()
        self.assertEqual(0, len(tx_service.get_tracked_blocks()))

