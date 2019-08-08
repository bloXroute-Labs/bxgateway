# pyre-ignore-all-errors
from typing import Tuple

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.node_type import NodeType
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.services.transaction_service import TransactionService
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection


class MockGatewayNode(AbstractGatewayNode):
    NODE_TYPE = NodeType.GATEWAY

    def __init__(self, opts):
        super(MockGatewayNode, self).__init__(opts)

        self.broadcast_messages = []
        self.send_to_node_messages = []
        self._tx_service = TransactionService(self, 0)

    def broadcast(self, msg, broadcasting_conn=None, prepend_to_queue=False, network_num=None,
                  connection_types=None, exclude_relays=False):
        if connection_types is None:
            connection_types = [ConnectionType.RELAY_ALL]

        self.broadcast_messages.append((msg, connection_types))
        return []

    def send_msg_to_node(self, msg):
        self.send_to_node_messages.append(msg)

    def get_tx_service(self, _network_num=None):
        return self._tx_service

    def build_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                    from_me: bool) -> AbstractGatewayBlockchainConnection:
        pass

    def build_relay_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                               from_me: bool) -> AbstractRelayConnection:
        pass

    def build_remote_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                           from_me: bool) -> AbstractGatewayBlockchainConnection:
        pass
