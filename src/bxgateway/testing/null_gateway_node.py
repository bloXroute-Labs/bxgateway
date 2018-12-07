import socket

from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.constants import LOCALHOST
from bxcommon.utils import logger
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class NullConnection(AbstractConnection):
    pass


class NullGatewayNode(AbstractGatewayNode):
    """
    Test Gateway Node that doesn't connect use its blockchain or relay connection.
    """
    def get_blockchain_connection_cls(self):
        return NullConnection

    def get_relay_connection_cls(self):
        return NullConnection

    def send_request_for_relay_peers(self):
        return 0

    def _send_request_for_gateway_peers(self):
        return 0

    def get_outbound_peer_addresses(self):
        return [(peer.ip, peer.port) for peer in self.outbound_peers]


class NullBlockchainNode(object):

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
