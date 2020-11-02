import time
from mock import MagicMock

from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.network.ip_endpoint import IpEndpoint
from bxgateway.testing import gateway_helpers
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import crypto
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash

from bxgateway.connections.ont.ont_base_connection_protocol import OntBaseConnectionProtocol
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.testing.mocks.mock_ont_gateway_node import MockOntGatewayNode
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service


class OntConnectionProtocolTest(AbstractTestCase):
    HASH = OntObjectHash(binary=crypto.double_sha256(b"123"))

    def setUp(self):
        opts = gateway_helpers.get_gateway_opts(8000, include_default_ont_args=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        self.node = MockOntGatewayNode(opts)
        self.node.block_processing_service = MagicMock()

        self.connection = MagicMock()
        gateway_helpers.add_blockchain_peer(self.node, self.connection)

        self.connection.node = self.node
        self.connection.peer_ip = LOCALHOST
        self.connection.peer_port = 8001
        self.connection.network_num = 2
        self.connection.endpoint = IpEndpoint(self.connection.peer_ip, self.connection.peer_port)
        self.node.blockchain_peers.add(BlockchainPeerInfo(self.connection.peer_ip, self.connection.peer_port))
        gateway_bdn_performance_stats_service.set_node(self.node)

        self.sut = OntBaseConnectionProtocol(self.connection)

    def test_msg_block_success(self):
        block_timestamp = int(time.time()) + 1 - \
                          self.node.opts.blockchain_ignore_block_interval_count * \
                          self.node.opts.blockchain_block_interval
        message = BlockOntMessage(0, 0, self.HASH, self.HASH, self.HASH, block_timestamp, 0, 0, bytes(10), bytes(20),
                                  [bytes(33)] * 5, [bytes(2)] * 3, [bytes(32)] * 5, self.HASH)

        self.sut.msg_block(message)
        self.node.block_processing_service.queue_block_for_processing.assert_called_once()

    def test_msg_block_too_old(self):
        block_timestamp = int(time.time()) - 1 - \
                          self.node.opts.blockchain_ignore_block_interval_count * \
                          self.node.opts.blockchain_block_interval
        message = BlockOntMessage(0, 0, self.HASH, self.HASH, self.HASH, block_timestamp, 0, 0, bytes(10), bytes(20),
                                  [bytes(33)] * 5, [bytes(2)] * 3, [bytes(32)] * 5, self.HASH)

        self.sut.msg_block(message)
        self.node.block_processing_service.queue_block_for_processing.assert_not_called()
