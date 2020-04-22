import weakref
from typing import TYPE_CHECKING

from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.ont.ont_node_connection_protocol import OntNodeConnectionProtocol

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class OntNodeConnection(AbstractGatewayBlockchainConnection):
    """
    bloXroute gateway <=> blockchain node connection class.
    """

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(OntNodeConnection, self).__init__(sock, node)
        self.connection_protocol = weakref.ref(OntNodeConnectionProtocol(self))
