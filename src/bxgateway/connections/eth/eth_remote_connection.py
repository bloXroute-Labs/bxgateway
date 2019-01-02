from bxcommon.connections.connection_state import ConnectionState
from bxgateway.connections.abstract_remote_blockchain_connection import AbstractRemoteBlockchainConnection
from bxgateway.connections.eth.eth_remote_connection_protocol import EthRemoteConnectionProtocol


class EthRemoteConnection(AbstractRemoteBlockchainConnection):
    def __init__(self, sock, address, node, from_me=False):
        super(EthRemoteConnection, self).__init__(sock, address, node, from_me)
        node_public_key = self.node.get_remote_public_key()
        is_handshake_initiator = node_public_key is not None
        private_key = self.node.get_private_key()
        self.connection_protocol = EthRemoteConnectionProtocol(self, is_handshake_initiator, private_key,
                                                               node_public_key)

    def enqueue_msg(self, msg, prepend=False):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        self.enqueue_msg_bytes(self.connection_protocol.get_message_bytes(msg), prepend)
