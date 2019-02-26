from bxcommon.constants import LOCALHOST
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_bx_messages import hello_message
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.testing.mocks.mock_btc_messages import btc_version_message


class AbstractBtcGatewayIntegrationTest(AbstractTestCase):
    """
    Abstract test class that setups up two gateways and allow test cases to pass messages back and forth.
    """

    def setUp(self):
        self.reinitialize_gateways(helpers.get_gateway_opts(9000, peer_gateways=[OutboundPeerModel(LOCALHOST, 7002)],
                                                            include_default_btc_args=True),
                                   helpers.get_gateway_opts(9001, peer_gateways=[OutboundPeerModel(LOCALHOST, 7002)],
                                                            include_default_btc_args=True))

    def reinitialize_gateways(self, opts1, opts2):
        self.node1 = BtcGatewayNode(opts1)
        self.node2 = BtcGatewayNode(opts2)

        self.node1.peer_gateways = {OutboundPeerModel(LOCALHOST, 7002)}
        self.node1.peer_relays = {OutboundPeerModel(LOCALHOST, 7001)}
        self.node2.peer_gateways = {OutboundPeerModel(LOCALHOST, 7002)}
        self.node2.peer_relays = {OutboundPeerModel(LOCALHOST, 7001)}

        self.blockchain_fileno = 1
        self.relay_fileno = 2
        self.gateway_fileno = 3

        self.blockchain_connection = MockSocketConnection(self.blockchain_fileno)
        self.relay_connection = MockSocketConnection(self.relay_fileno)
        self.gateway_connection = MockSocketConnection(self.gateway_fileno)

        # add node1 connections
        self.node1.on_connection_added(self.blockchain_connection, LOCALHOST, 7000, True)
        self.node1.on_connection_added(self.relay_connection, LOCALHOST, 7001, True)
        self.node1.on_connection_initialized(self.relay_fileno)
        self.node1.on_connection_added(self.gateway_connection, LOCALHOST, 7002, True)

        # add node 2 connections
        self.node2.on_connection_added(self.blockchain_connection, LOCALHOST, 7000, True)
        self.node2.on_connection_added(self.relay_connection, LOCALHOST, 7001, True)
        self.node2.on_connection_initialized(self.relay_fileno)
        self.node2.on_connection_added(self.gateway_connection, LOCALHOST, 7002, True)

        # initialize node1 connections
        helpers.receive_node_message(self.node1, self.blockchain_fileno, btc_version_message().rawbytes())
        helpers.receive_node_message(self.node1, self.relay_fileno, hello_message().rawbytes())
        helpers.receive_node_message(self.node1, self.relay_fileno, AckMessage().rawbytes())
        helpers.receive_node_message(self.node1, self.gateway_fileno, AckMessage().rawbytes())

        # initialize node2 connections
        helpers.receive_node_message(self.node2, self.blockchain_fileno, btc_version_message().rawbytes())
        helpers.receive_node_message(self.node2, self.relay_fileno, hello_message().rawbytes())
        helpers.receive_node_message(self.node2, self.relay_fileno, AckMessage().rawbytes())
        helpers.receive_node_message(self.node2, self.gateway_fileno, AckMessage().rawbytes())

        self.clear_all_buffers()

    def clear_all_buffers(self):
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)
        helpers.clear_node_buffer(self.node1, self.relay_fileno)
        helpers.clear_node_buffer(self.node1, self.gateway_fileno)
        helpers.clear_node_buffer(self.node2, self.blockchain_fileno)
        helpers.clear_node_buffer(self.node2, self.relay_fileno)
        helpers.clear_node_buffer(self.node2, self.gateway_fileno)
