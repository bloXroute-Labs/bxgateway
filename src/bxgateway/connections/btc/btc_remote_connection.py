import weakref

from bxcommon.connections.connection_type import ConnectionType
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.btc.btc_remote_connection_protocol import BtcRemoteConnectionProtocol


class BtcRemoteConnection(AbstractGatewayBlockchainConnection):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, socket_connection, address, node, from_me=False):
        super(BtcRemoteConnection, self).__init__(socket_connection, address, node, from_me)
        self.connection_protocol = weakref.ref(BtcRemoteConnectionProtocol(self))
