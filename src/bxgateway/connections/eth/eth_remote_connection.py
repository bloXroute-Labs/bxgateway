from bxcommon.connections.connection_type import ConnectionType
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_remote_connection_protocol import EthRemoteConnectionProtocol


class EthRemoteConnection(EthBaseConnection):
    CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

    def __init__(self, sock, address, node, from_me=False):
        super(EthRemoteConnection, self).__init__(sock, address, node, from_me)
        node_public_key = self.node.get_remote_public_key()
        is_handshake_initiator = node_public_key is not None
        private_key = self.node.get_private_key()
        self.connection_protocol = EthRemoteConnectionProtocol(self, is_handshake_initiator, private_key,
                                                               node_public_key)
