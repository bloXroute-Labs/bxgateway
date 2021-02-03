import weakref
from typing import TYPE_CHECKING

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.ont.ont_node_connection_protocol import OntNodeConnectionProtocol
from bxgateway.messages.ont.ont_message_factory import ont_message_factory
from bxgateway.messages.ont.ping_ont_message import PingOntMessage

if TYPE_CHECKING:
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode


class OntNodeConnection(AbstractGatewayBlockchainConnection["OntGatewayNode"]):
    """
    bloXroute gateway <=> blockchain node connection class.
    """

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "OntGatewayNode"):
        super(OntNodeConnection, self).__init__(sock, node)
        self.connection_protocol = weakref.ref(OntNodeConnectionProtocol(self))

    def connection_message_factory(self) -> AbstractMessageFactory:
        return ont_message_factory

    def ping_message(self) -> AbstractMessage:
        return PingOntMessage(
            self.node.opts.blockchain_net_magic, height=self.node.current_block_height
        )
