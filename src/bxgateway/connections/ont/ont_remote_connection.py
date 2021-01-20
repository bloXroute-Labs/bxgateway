import weakref
from typing import TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.ont.ont_remote_connection_protocol import OntRemoteConnectionProtocol
from bxgateway.messages.ont.ont_message_factory import ont_message_factory
from bxgateway.messages.ont.ping_ont_message import PingOntMessage

if TYPE_CHECKING:
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode


class OntRemoteConnection(AbstractGatewayBlockchainConnection["OntGatewayNode"]):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, socket_connection: AbstractSocketConnectionProtocol, node: "OntGatewayNode"):
        super(OntRemoteConnection, self).__init__(socket_connection, node)
        self.connection_protocol = weakref.ref(OntRemoteConnectionProtocol(self))

    def connection_message_factory(self) -> AbstractMessageFactory:
        return ont_message_factory

    def ping_message(self) -> AbstractMessage:
        return PingOntMessage(
            self.node.opts.blockchain_net_magic, height=self.node.current_block_height
        )
