import weakref
from typing import TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.btc.btc_remote_connection_protocol import BtcRemoteConnectionProtocol
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class BtcRemoteConnection(AbstractGatewayBlockchainConnection["AbstractGatewayNode"]):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, socket_connection: AbstractSocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(BtcRemoteConnection, self).__init__(socket_connection, node)
        self.connection_protocol = weakref.ref(BtcRemoteConnectionProtocol(self))

    def ping_message(self) -> AbstractMessage:
        return PingBtcMessage(self.node.opts.blockchain_net_magic)
