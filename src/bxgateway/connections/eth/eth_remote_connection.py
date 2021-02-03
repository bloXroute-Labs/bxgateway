from typing import TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_remote_connection_protocol import EthRemoteConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode


class EthRemoteConnection(EthBaseConnection):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "EthGatewayNode"):
        super(EthRemoteConnection, self).__init__(sock, node)
        self.connection_protocol = EthRemoteConnectionProtocol(
            self, self.is_handshake_initiator, self.rlpx_cipher
        )

    def connection_public_key(
        self, _sock: AbstractSocketConnectionProtocol, node: "EthGatewayNode"
    ) -> bytes:
        return node.get_remote_public_key()
