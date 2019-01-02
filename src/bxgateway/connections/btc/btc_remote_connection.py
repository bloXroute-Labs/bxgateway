from bxgateway.connections.abstract_remote_blockchain_connection import AbstractRemoteBlockchainConnection
from bxgateway.connections.btc.btc_remote_connection_protocol import BtcRemoteConnectionProtocol


class BtcRemoteConnection(AbstractRemoteBlockchainConnection):
    def __init__(self, socket_connection, address, node, from_me=False):
        super(BtcRemoteConnection, self).__init__(socket_connection, address, node, from_me)
        self.connection_protocol = BtcRemoteConnectionProtocol(self)
