import weakref
from typing import TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.ont.ont_remote_connection_protocol import OntRemoteConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class OntRemoteConnection(AbstractGatewayBlockchainConnection["AbstractGatewayNode"]):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, socket_connection: SocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(OntRemoteConnection, self).__init__(socket_connection, node)
        self.connection_protocol = weakref.ref(OntRemoteConnectionProtocol(self))
