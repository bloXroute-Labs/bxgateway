from typing import TYPE_CHECKING

from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.connection_type import ConnectionType
from bxgateway import gateway_constants

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class AbstractGatewayBlockchainConnection(AbstractConnection["AbstractGatewayNode"]):
    CONNECTION_TYPE = ConnectionType.BLOCKCHAIN_NODE

    def __init__(self, sock, address, node, from_me=False):
        super(AbstractGatewayBlockchainConnection, self).__init__(sock, address, node, from_me)
        self.message_converter = None
        self.connection_protocol = None
        self.is_server = False

    def send_ping(self):
        self.enqueue_msg(self.ping_message)
        return gateway_constants.BLOCKCHAIN_PING_INTERVAL_S
