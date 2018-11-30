from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection


class BtcGatewayNode(AbstractGatewayNode):
    relay_connection_cls = BtcRelayConnection

    def __init__(self, opts):
        super(BtcGatewayNode, self).__init__(opts)