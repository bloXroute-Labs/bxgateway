from typing import TYPE_CHECKING

from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode


class EthNodeConnection(EthBaseConnection):
    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "EthGatewayNode"):
        super(EthNodeConnection, self).__init__(sock, node)

        node_public_key = self.node.get_node_public_key(self.peer_ip, self.peer_port)
        is_handshake_initiator = node_public_key is not None
        private_key = self.node.get_private_key()
        self.connection_protocol = EthNodeConnectionProtocol(
            self, is_handshake_initiator, private_key, node_public_key
        )

    def get_connection_state_details(self):
        """
        Returns details of the current connection state. Used to submit details to SDN on disconnect.
        :return:
        """
        return f"Connection state: {self.state}. Protocol status: {self.connection_protocol.connection_status}."
