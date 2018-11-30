from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection


class BtcGatewayNode(AbstractGatewayNode):
    def __init__(self, opts):
        super(BtcGatewayNode, self).__init__(opts)

    def get_blockchain_connection_cls(self):
        return BtcNodeConnection

    def get_relay_connection_cls(self):
        return BtcRelayConnection

