import weakref
from typing import TYPE_CHECKING

from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.btc.btc_node_connection_protocol import BtcNodeConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class BtcNodeConnection(AbstractGatewayBlockchainConnection):
    """
    bloXroute gateway <=> blockchain node connection class.
    """

    def __init__(self, sock: SocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(BtcNodeConnection, self).__init__(sock, node)
        self.connection_protocol = weakref.ref(BtcNodeConnectionProtocol(self))
