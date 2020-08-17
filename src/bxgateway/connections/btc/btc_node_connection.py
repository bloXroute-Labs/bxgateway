import weakref
from typing import TYPE_CHECKING

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.btc.btc_node_connection_protocol import BtcNodeConnectionProtocol
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class BtcNodeConnection(AbstractGatewayBlockchainConnection):
    """
    bloXroute gateway <=> blockchain node connection class.
    """

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(BtcNodeConnection, self).__init__(sock, node)
        self.connection_protocol = weakref.ref(BtcNodeConnectionProtocol(self))

    def ping_message(self) -> AbstractMessage:
        return PingBtcMessage(self.node.opts.blockchain_net_magic)
