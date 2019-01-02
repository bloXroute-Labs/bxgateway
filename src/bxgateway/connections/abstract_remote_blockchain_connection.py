from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.connection_type import ConnectionType
from bxgateway import gateway_constants


class AbstractRemoteBlockchainConnection(AbstractConnection):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, socket_connection, address, node, from_me=False):
        super(AbstractRemoteBlockchainConnection, self).__init__(socket_connection, address, node, from_me)

        self.connection_protocol = None

    def send_ping(self):
        self.enqueue_msg(self.ping_message)
        return gateway_constants.BLOCKCHAIN_PING_INTERVAL_S
