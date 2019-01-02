from bxcommon.connections.connection_state import ConnectionState
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol


class EthNodeConnection(AbstractGatewayBlockchainConnection):
    def __init__(self, sock, address, node, from_me=False):
        super(EthNodeConnection, self).__init__(sock, address, node, from_me)

        node_public_key = self.node.get_node_public_key()
        is_handshake_initiator = node_public_key is not None
        private_key = self.node.get_private_key()
        self.connection_protocol = EthNodeConnectionProtocol(self, is_handshake_initiator, private_key,
                                                             node_public_key)

    def enqueue_msg(self, msg, prepend=False):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        self.enqueue_msg_bytes(self.connection_protocol.get_message_bytes(msg), prepend)
