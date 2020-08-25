# pyre-ignore-all-errors
import socket
from argparse import Namespace
from typing import Optional, List

from mock import MagicMock

from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.constants import LOCALHOST
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.network.peer_info import ConnectionPeerInfo
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.utils import convert
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.push_block_queuing_service import PushBlockQueuingService
from bxutils import logging
from bxutils.services.node_ssl_service import NodeSSLService

logger = logging.get_logger(__name__)


class NullConnection(AbstractConnection):
    pass


# noinspection PyTypeChecker
class NullGatewayNode(AbstractGatewayNode):
    """
    Test Gateway Node that doesn't connect use its blockchain or relay connection.
    """

    def __init__(self, opts: Namespace, node_ssl_service: Optional[NodeSSLService] = None):
        if node_ssl_service is None:
            node_ssl_service = MockNodeSSLService(self.NODE_TYPE, MagicMock())
        helpers.set_extensions_parallelism()
        super().__init__(opts, node_ssl_service)

    def build_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        return NullConnection

    def build_relay_connection(self, socket_connection: AbstractSocketConnectionProtocol) -> AbstractRelayConnection:
        return NullConnection

    def build_remote_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        return NullConnection

    def sync_and_send_request_for_relay_peers(self, network_num: int):
        return 0

    def build_block_queuing_service(self) -> PushBlockQueuingService:
        pass

    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        pass

    def _send_request_for_gateway_peers(self):
        return 0

    def get_outbound_peer_info(self) -> List[ConnectionPeerInfo]:
        return [ConnectionPeerInfo(
            IpEndpoint(peer.ip, peer.port),
            convert.peer_node_to_connection_type(self.NODE_TYPE, peer.node_type)) for peer in self.outbound_peers
        ]


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
