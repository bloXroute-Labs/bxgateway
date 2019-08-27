# pyre-ignore-all-errors

import socket
from argparse import Namespace
from typing import Tuple

from bxcommon.test_utils import helpers

from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.constants import LOCALHOST
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.utils import logger
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection


class NullConnection(AbstractConnection):
    pass


# noinspection PyTypeChecker
class NullGatewayNode(AbstractGatewayNode):
    """
    Test Gateway Node that doesn't connect use its blockchain or relay connection.
    """
    def __init__(self, opts: Namespace):
        helpers.set_extensions_parallelism()
        super().__init__(opts)

    def build_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                    from_me: bool) -> AbstractGatewayBlockchainConnection:
        return NullConnection

    def build_relay_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                               from_me: bool) -> AbstractRelayConnection:
        return NullConnection

    def build_remote_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                           from_me: bool) -> AbstractGatewayBlockchainConnection:
        return NullConnection

    def send_request_for_relay_peers(self):
        return 0

    def _send_request_for_gateway_peers(self):
        return 0

    def get_outbound_peer_addresses(self):
        return [(peer.ip, peer.port) for peer in self.outbound_peers]


class NullBlockchainNode:

    def __init__(self, port):
        logger.debug("Starting null blockchain node on {}".format(port))

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((LOCALHOST, port))
        self._sock.listen(50)

        self.connection = None

    def accept(self):
        self.connection, address = self._sock.accept()
        logger.debug("Null blockchain got a connection from {}".format(address))
        return address
