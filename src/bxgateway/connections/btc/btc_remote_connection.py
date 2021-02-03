import weakref
from typing import TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.btc.btc_remote_connection_protocol import BtcRemoteConnectionProtocol
from bxgateway.messages.btc.btc_message_factory import btc_message_factory
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage

if TYPE_CHECKING:
    from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode


class BtcRemoteConnection(AbstractGatewayBlockchainConnection["BtcGatewayNode"]):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "BtcGatewayNode"):
        super().__init__(sock, node)
        self.connection_protocol = weakref.ref(BtcRemoteConnectionProtocol(self))

    def connection_message_factory(self) -> AbstractMessageFactory:
        return btc_message_factory

    def ping_message(self) -> AbstractMessage:
        return PingBtcMessage(self.node.opts.blockchain_net_magic)
