from typing import TYPE_CHECKING

from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode


class EthNodeConnection(EthBaseConnection):
    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "EthGatewayNode"):
        super().__init__(sock, node)
        self.connection_protocol = EthNodeConnectionProtocol(
            self, self.is_handshake_initiator, self.rlpx_cipher
        )

    def connection_public_key(
        self, sock: AbstractSocketConnectionProtocol, node: "EthGatewayNode"
    ) -> bytes:
        peer_ip, peer_port = sock.endpoint
        return node.get_node_public_key(peer_ip, peer_port)

    def get_connection_state_details(self):
        """
        Returns details of the current connection state. Used to submit details to SDN on disconnect.
        :return:
        """
        return f"Connection state: {self.state}. Protocol status: {self.connection_protocol.connection_status}."
