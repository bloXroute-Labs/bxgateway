from mock import MagicMock

from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import crypto
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.connections.btc.btc_base_connection_protocol import BtcBaseConnectionProtocol
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcConnectionProtocolTest(AbstractTestCase):
    HASH = BtcObjectHash(binary=crypto.double_sha256("123"))

    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000, include_default_btc_args=True))
        self.node.neutrality_service = MagicMock()

        self.connection = MagicMock()
        self.connection.node = self.node
        self.connection.peer_ip = LOCALHOST
        self.connection.peer_port = 8001

        self.sut = BtcBaseConnectionProtocol(self.connection)

    def test_msg_block(self):
        txns = [TxBtcMessage(0, 0, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]
        message = BlockBtcMessage(0, 0, self.HASH, self.HASH, 0, 0, 0, txns)
        self.sut.msg_block(message)
        self.node.neutrality_service.propagate_block_to_network.assert_called_once()
