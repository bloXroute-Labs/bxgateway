from typing import TYPE_CHECKING

from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode


class EthNodeConnection(EthBaseConnection):
    def __init__(self, sock: SocketConnectionProtocol, node: "EthGatewayNode"):
        super(EthNodeConnection, self).__init__(sock, node)

        node_public_key = self.node.get_node_public_key()
        is_handshake_initiator = node_public_key is not None
        private_key = self.node.get_private_key()
        self.connection_protocol = EthNodeConnectionProtocol(
            self, is_handshake_initiator, private_key, node_public_key
        )
