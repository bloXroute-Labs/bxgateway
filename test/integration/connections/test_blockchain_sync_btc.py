from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.utils import crypto
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash

from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_remote_connection import BtcRemoteConnection
from bxgateway.messages.btc.data_btc_message import GetHeadersBtcMessage
from bxgateway.messages.btc.headers_btc_message import HeadersBtcMessage
from bxgateway.testing import spies


class BlockchainSyncBtcTest(AbstractTestCase):
    HASH = BtcObjectHash(binary=crypto.bitcoin_hash(b"hi"))

    def setUp(self):
        self.local_node_fileno = 1
        self.remote_node_fileno = 2

        self.gateway_node = spies.make_spy_node(BtcGatewayNode, 8000, include_default_btc_args=True)
        self.btc_node_connection = spies.make_spy_connection(BtcNodeConnection, self.local_node_fileno, 8001,
                                                             self.gateway_node)
        self.btc_remote_node_connection = spies.make_spy_connection(BtcRemoteConnection, self.remote_node_fileno, 8002,
                                                                    self.gateway_node)
        self.gateway_node.node_conn = self.btc_node_connection
        self.gateway_node.remote_node_conn = self.btc_remote_node_connection
        self.gateway_node.connection_pool.add(self.local_node_fileno, LOCALHOST, 8001, self.btc_node_connection)
        self.gateway_node.connection_pool.add(self.remote_node_fileno, LOCALHOST, 8002, self.btc_remote_node_connection)

    def test_block_headers_request(self):
        sent_get_headers = GetHeadersBtcMessage(12345, 23456, [self.HASH], self.HASH)
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, sent_get_headers.rawbytes())
        self.btc_remote_node_connection.enqueue_msg.assert_called_once_with(sent_get_headers)

        response_headers = HeadersBtcMessage(12345, [])
        helpers.receive_node_message(self.gateway_node, self.remote_node_fileno, response_headers.rawbytes())
        self.btc_node_connection.enqueue_msg.assert_called_once_with(response_headers)
