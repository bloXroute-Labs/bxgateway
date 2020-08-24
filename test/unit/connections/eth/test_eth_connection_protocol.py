import time

from mock import MagicMock

from bxgateway.testing import gateway_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers

from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxcommon.utils.blockchain_utils.eth import crypto_utils


def _block_with_timestamp(timestamp):
    nonce = 5
    header = mock_eth_messages.get_dummy_block_header(5, int(timestamp))
    block = mock_eth_messages.get_dummy_block(nonce, header)
    return block


class EthConnectionProtocolTest(AbstractTestCase):

    def setUp(self):
        opts = gateway_helpers.get_gateway_opts(
            8000,
            include_default_eth_args=True,
            track_detailed_sent_messages=True
        )
        if opts.use_extensions:
            helpers.set_extensions_parallelism()

        self.node = MockGatewayNode(opts)
        self.node.block_processing_service = MagicMock()

        self.connection = MagicMock()
        self.connection.node = self.node
        self.connection.peer_ip = LOCALHOST
        self.connection.peer_port = 8001
        self.connection.network_num = 2

        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)
        self.sut = EthBaseConnectionProtocol(self.connection, True, dummy_private_key, dummy_public_key)

    def test_msg_block_success(self):
        message = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() + 1 - self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
            ),
            10
        )
        message.serialize()
        self.sut.msg_block(message)
        self.node.block_processing_service.queue_block_for_processing.assert_called_once()

    def test_msg_block_too_old(self):
        message = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() - 1 - self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
            ),
            10
        )
        message.serialize()
        self.sut.msg_block(message)
        self.node.block_processing_service.queue_block_for_processing.assert_not_called()

    def test_msg_block_in_recovery(self):
        message = NewBlockEthProtocolMessage(None, _block_with_timestamp(time.time()), 10)
        self.node.block_recovery_service.add_block(
            message.rawbytes(), message.block_hash(), [1], []
        )
        self.sut.msg_block(message)
        self.node.block_processing_service.queue_block_for_processing.assert_not_called()
