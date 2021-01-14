from typing import Type, NamedTuple

from mock import MagicMock

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.gateway_opts import GatewayOpts


class FullGatewayInfo(NamedTuple):
    gateway: AbstractGatewayNode

    blockchain_fileno: int
    relay_fileno: int
    gateway_fileno: int

    blockchain_socket: AbstractSocketConnectionProtocol
    relay_socket: AbstractSocketConnectionProtocol
    gateway_socket: AbstractSocketConnectionProtocol

    blockchain_connection: AbstractGatewayBlockchainConnection
    relay_connection: AbstractRelayConnection
    gateway_connection: GatewayConnection


def of_type(gateway_class: Type[AbstractGatewayNode], opts: GatewayOpts) -> FullGatewayInfo:
    # probably pyre bug
    # pyre-fixme[45]: Cannot instantiate abstract class `AbstractGatewayNode`.
    gateway = gateway_class(opts, MockNodeSSLService(gateway_class.NODE_TYPE, MagicMock()))
    gateway.requester = MagicMock()

    blockchain_fileno = 1
    relay_fileno = 2
    gateway_fileno = 3

    blockchain_socket = MockSocketConnection(
        file_no=blockchain_fileno,
        node=gateway,
        ip_address=constants.LOCALHOST,
        port=7000
    )
    relay_socket = MockSocketConnection(
        file_no=relay_fileno,
        node=gateway,
        ip_address=constants.LOCALHOST,
        port=7001
    )
    gateway_socket = MockSocketConnection(
        file_no=gateway_fileno,
        node=gateway,
        ip_address=constants.LOCALHOST,
        port=7002
    )

    gateway.on_connection_added(blockchain_socket)

    gateway.peer_relays.add(OutboundPeerModel(constants.LOCALHOST, 7001))
    gateway.on_connection_added(relay_socket)

    gateway.peer_gateways.add(OutboundPeerModel(constants.LOCALHOST, 7002))
    gateway.on_connection_added(gateway_socket)

    blockchain_connection = gateway.connection_pool.get_by_fileno(blockchain_fileno)
    assert blockchain_connection is not None
    assert isinstance(blockchain_connection, AbstractGatewayBlockchainConnection)
    relay_connection = gateway.connection_pool.get_by_fileno(relay_fileno)
    assert relay_connection is not None
    assert isinstance(relay_connection, AbstractRelayConnection)
    gateway_connection = gateway.connection_pool.get_by_fileno(gateway_fileno)
    assert gateway_connection is not None
    assert isinstance(gateway_connection, GatewayConnection)

    gateway.on_blockchain_connection_ready(blockchain_connection)
    blockchain_connection.state |= ConnectionState.ESTABLISHED
    relay_connection.state |= ConnectionState.ESTABLISHED
    gateway_connection.state |= ConnectionState.ESTABLISHED

    # clear output buffers
    if blockchain_connection.outputbuf.length:
        blockchain_connection.advance_sent_bytes(blockchain_connection.outputbuf.length)

    if relay_connection.outputbuf.length:
        relay_connection.advance_sent_bytes(relay_connection.outputbuf.length)

    if gateway_connection.outputbuf.length:
        gateway_connection.advance_sent_bytes(gateway_connection.outputbuf.length)

    return FullGatewayInfo(
        gateway,
        blockchain_fileno,
        relay_fileno,
        gateway_fileno,
        blockchain_socket,
        relay_socket,
        gateway_socket,
        blockchain_connection,
        relay_connection,
        gateway_connection
    )
