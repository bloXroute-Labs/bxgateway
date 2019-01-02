from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.btc.btc_node_connection_protocol import BtcNodeConnectionProtocol


class BtcNodeConnection(AbstractGatewayBlockchainConnection):
    """
    bloXroute gateway <=> blockchain node connection class.
    """

    def __init__(self, sock, address, node, from_me=False):
        super(BtcNodeConnection, self).__init__(sock, address, node, from_me)
        self.connection_protocol = BtcNodeConnectionProtocol(self)
