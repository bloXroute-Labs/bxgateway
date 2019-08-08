import time
from typing import Tuple

from mock import MagicMock, call

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import LOCALHOST, CONNECTION_RETRY_SECONDS, MAX_CONNECT_RETRIES, SDN_CONTACT_RETRY_SECONDS
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.services import sdn_http_service
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils.alarm_queue import AlarmQueue
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection
from bxgateway.connections.btc.btc_remote_connection import BtcRemoteConnection
from bxgateway.utils import network_latency


class GatewayNode(AbstractGatewayNode):
    def build_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                    from_me: bool) -> AbstractGatewayBlockchainConnection:
        return BtcNodeConnection(socket_connection, address, self, from_me)

    def build_relay_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                               from_me: bool) -> AbstractRelayConnection:
        return BtcRelayConnection(socket_connection, address, self, from_me)

    def build_remote_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                           from_me: bool) -> AbstractGatewayBlockchainConnection:
        return BtcRemoteConnection(socket_connection, address, self, from_me)


def initialize_split_relay_node():
    relay_connections = [OutboundPeerModel(LOCALHOST, 8001)]
    sdn_http_service.fetch_potential_relay_peers = MagicMock(return_value=relay_connections)
    network_latency.get_best_relay_by_ping_latency = MagicMock(return_value=relay_connections[0])

    node = GatewayNode(helpers.get_gateway_opts(8000, split_relays=True, include_default_btc_args=True))
    node.enqueue_connection = MagicMock()
    node.enqueue_disconnect = MagicMock()

    node.send_request_for_relay_peers()
    node.enqueue_connection.assert_has_calls([
        call(LOCALHOST, 8001),
        call(LOCALHOST, 8002),
    ], any_order=True)
    node.on_connection_added(MockSocketConnection(1), LOCALHOST, 8001, True)
    node.on_connection_added(MockSocketConnection(2), LOCALHOST, 8002, True)

    node.alarm_queue = AlarmQueue()
    node.enqueue_connection.reset_mock()
    return node


class AbstractGatewayNodeTest(AbstractTestCase):

    def test_gateway_peer_sdn_update(self):
        # handle duplicates, keeps old, self ip, and maintain peers from CLI
        peer_gateways = [
            OutboundPeerModel(LOCALHOST, 8001),
            OutboundPeerModel(LOCALHOST, 8002)
        ]
        node = GatewayNode(helpers.get_gateway_opts(8000, peer_gateways=peer_gateways))
        node.peer_gateways.add(OutboundPeerModel(LOCALHOST, 8004))

        sdn_http_service.fetch_gateway_peers = MagicMock(return_value=[
            OutboundPeerModel(LOCALHOST, 8000),
            OutboundPeerModel(LOCALHOST, 8001),
            OutboundPeerModel(LOCALHOST, 8003)
        ])

        node._send_request_for_gateway_peers()
        self.assertEqual(4, len(node.outbound_peers))
        self.assertEqual(4, len(node.peer_gateways))
        self.assertNotIn(OutboundPeerModel(LOCALHOST, 8000), node.peer_gateways)
        self.assertIn(OutboundPeerModel(LOCALHOST, 8001), node.peer_gateways)
        self.assertIn(OutboundPeerModel(LOCALHOST, 8002), node.peer_gateways)
        self.assertIn(OutboundPeerModel(LOCALHOST, 8003), node.peer_gateways)
        self.assertIn(OutboundPeerModel(LOCALHOST, 8004), node.peer_gateways)

    def test_gateway_peer_never_destroy_cli_peer(self):
        peer_gateways = [
            OutboundPeerModel(LOCALHOST, 8001)
        ]
        node = GatewayNode(helpers.get_gateway_opts(8000, peer_gateways=peer_gateways))
        node.enqueue_connection = MagicMock()
        node.alarm_queue.register_alarm = MagicMock()
        node.enqueue_disconnect = MagicMock()

        node.on_connection_added(MockSocketConnection(), LOCALHOST, 8001, True)
        cli_peer_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8001)
        node.num_retries_by_ip[(LOCALHOST, 8001)] = MAX_CONNECT_RETRIES
        node._connection_timeout(cli_peer_conn)

        node.alarm_queue.register_alarm.assert_has_calls([call(CONNECTION_RETRY_SECONDS, node._retry_init_client_socket,
                                                               LOCALHOST, 8001, ConnectionType.GATEWAY)])
        node._retry_init_client_socket(LOCALHOST, 8001, ConnectionType.GATEWAY)
        self.assertEqual(MAX_CONNECT_RETRIES + 1, node.num_retries_by_ip[(LOCALHOST, 8001)])

    def test_gateway_peer_get_more_peers_when_too_few_gateways(self):
        peer_gateways = [
            OutboundPeerModel(LOCALHOST, 8001),
        ]
        node = GatewayNode(helpers.get_gateway_opts(8000, peer_gateways=peer_gateways, min_peer_gateways=2))
        node.peer_gateways.add(OutboundPeerModel(LOCALHOST, 8002))
        sdn_http_service.fetch_gateway_peers = MagicMock(return_value=[
            OutboundPeerModel(LOCALHOST, 8003, "12345")
        ])

        node.on_connection_added(MockSocketConnection(), LOCALHOST, 8002, True)
        not_cli_peer_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8002)
        node.destroy_conn(not_cli_peer_conn)

        time.time = MagicMock(return_value=time.time() + SDN_CONTACT_RETRY_SECONDS * 2)
        node.alarm_queue.fire_alarms()

        sdn_http_service.fetch_gateway_peers.assert_has_calls([call(node.opts.node_id), call(node.opts.node_id)])
        self.assertEqual(2, len(node.outbound_peers))
        self.assertIn(OutboundPeerModel(LOCALHOST, 8001), node.outbound_peers)
        self.assertIn(OutboundPeerModel(LOCALHOST, 8003, "12345"), node.outbound_peers)

    def test_get_preferred_gateway_connection_opts(self):
        opts_peer_gateways = [
            OutboundPeerModel(LOCALHOST, 8001)
        ]
        node = GatewayNode(helpers.get_gateway_opts(8000, peer_gateways=opts_peer_gateways))
        node.on_connection_added(MockSocketConnection(), LOCALHOST, 8001, False)

        opts_connection = node.connection_pool.get_by_ipport(LOCALHOST, 8001)
        self.assertIsNone(node.get_preferred_gateway_connection())
        opts_connection.state |= ConnectionState.ESTABLISHED
        self.assertEqual(opts_connection, node.get_preferred_gateway_connection())

    def test_get_preferred_gateway_connection_bloxroute(self):
        node = GatewayNode(helpers.get_gateway_opts(8000))
        node.peer_gateways = [
            OutboundPeerModel(LOCALHOST, 8001, is_internal_gateway=False),
            OutboundPeerModel(LOCALHOST, 8002, is_internal_gateway=True)
        ]

        node.on_connection_added(MockSocketConnection(), LOCALHOST, 8001, False)
        node.on_connection_added(MockSocketConnection(), LOCALHOST, 8002, False)

        other_connection = node.connection_pool.get_by_ipport(LOCALHOST, 8001)
        other_connection.state |= ConnectionState.ESTABLISHED
        bloxroute_connection = node.connection_pool.get_by_ipport(LOCALHOST, 8002)
        bloxroute_connection.state |= ConnectionState.ESTABLISHED
        self.assertEqual(bloxroute_connection, node.get_preferred_gateway_connection())

        bloxroute_connection.mark_for_close()
        self.assertEqual(other_connection, node.get_preferred_gateway_connection())

    def test_split_relay_connection(self):
        relay_connections = [OutboundPeerModel(LOCALHOST, 8001)]
        sdn_http_service.fetch_potential_relay_peers = MagicMock(return_value=relay_connections)
        network_latency.get_best_relay_by_ping_latency = MagicMock(return_value=relay_connections[0])

        node = GatewayNode(helpers.get_gateway_opts(8000, split_relays=True, include_default_btc_args=True))
        node.enqueue_connection = MagicMock()

        node.send_request_for_relay_peers()
        self.assertEqual(1, len(node.peer_relays))
        self.assertEqual(1, len(node.peer_transaction_relays))
        self.assertEqual(8002, next(iter(node.peer_transaction_relays)).port)

        node.enqueue_connection.assert_has_calls([
            call(LOCALHOST, 8001),
            call(LOCALHOST, 8002),
        ], any_order=True)

        node.on_connection_added(MockSocketConnection(1), LOCALHOST, 8001, True)
        self.assertEqual(1, len(node.connection_pool))
        self.assertEqual(1, len(node.connection_pool.get_by_connection_type(ConnectionType.RELAY_BLOCK)))
        self.assertEqual(0, len(node.connection_pool.get_by_connection_type(ConnectionType.RELAY_TRANSACTION)))
        self.assertEqual(1, len(node.connection_pool.get_by_connection_type(ConnectionType.RELAY_ALL)))

        node.on_connection_added(MockSocketConnection(2), LOCALHOST, 8002, True)
        self.assertEqual(2, len(node.connection_pool))
        self.assertEqual(1, len(node.connection_pool.get_by_connection_type(ConnectionType.RELAY_BLOCK)))
        self.assertEqual(1, len(node.connection_pool.get_by_connection_type(ConnectionType.RELAY_TRANSACTION)))
        self.assertEqual(2, len(node.connection_pool.get_by_connection_type(ConnectionType.RELAY_ALL)))

    def test_split_relay_reconnect_disconnect_block(self):
        node = initialize_split_relay_node()

        relay_block_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8001)
        node.on_connection_closed(relay_block_conn.fileno)

        self.assertEqual(1, len(node.alarm_queue.alarms))
        self.assertEqual(node._retry_init_client_socket, node.alarm_queue.alarms[0].alarm.fn)
        self.assertEqual(True, node.should_retry_connection(LOCALHOST, 8001, relay_block_conn.CONNECTION_TYPE))
        node.enqueue_disconnect.assert_called_once_with(relay_block_conn.fileno)

        time.time = MagicMock(return_value=time.time() + constants.CONNECTION_RETRY_SECONDS)
        node.alarm_queue.fire_alarms()
        node.enqueue_connection.assert_called_once_with(LOCALHOST, 8001)

    def test_split_relay_reconnect_disconnect_transaction(self):
        node = initialize_split_relay_node()

        relay_tx_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8002)
        node.on_connection_closed(relay_tx_conn.fileno)

        self.assertEqual(1, len(node.alarm_queue.alarms))
        self.assertEqual(node._retry_init_client_socket, node.alarm_queue.alarms[0].alarm.fn)
        self.assertEqual(True, node.should_retry_connection(LOCALHOST, 8002, relay_tx_conn.CONNECTION_TYPE))
        node.enqueue_disconnect.assert_called_once_with(relay_tx_conn.fileno)

        time.time = MagicMock(return_value=time.time() + constants.CONNECTION_RETRY_SECONDS)
        node.alarm_queue.fire_alarms()
        node.enqueue_connection.assert_called_once_with(LOCALHOST, 8002)

    def test_split_relay_no_reconnect_disconnect_block(self):
        sdn_http_service.submit_peer_connection_error_event = MagicMock()
        node = initialize_split_relay_node()
        node.num_retries_by_ip[(LOCALHOST, 8001)] = constants.MAX_CONNECT_RETRIES

        relay_block_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8001)
        relay_transaction_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8002)
        self.assertEqual(False, node.should_retry_connection(LOCALHOST, 8001, relay_block_conn.CONNECTION_TYPE))
        self.assertEqual(True, node.should_retry_connection(LOCALHOST, 8002, relay_transaction_conn.CONNECTION_TYPE))

        node.on_connection_closed(relay_block_conn.fileno)
        self.assertEqual(1, len(node.alarm_queue.alarms))
        self.assertEqual(node._retry_init_client_socket, node.alarm_queue.alarms[0].alarm.fn)
        node.enqueue_disconnect.assert_called_once_with(relay_block_conn.fileno)
        node.enqueue_disconnect.reset_mock()

        time.time = MagicMock(return_value=time.time() + constants.CONNECTION_RETRY_SECONDS)
        node.alarm_queue.fire_alarms()

        node.enqueue_connection.assert_not_called()
        sdn_http_service.submit_peer_connection_error_event.assert_has_calls(
            [
                call(node.opts.node_id, LOCALHOST, 8001),
                call(node.opts.node_id, LOCALHOST, 8002),
            ],
            any_order=True
        )
        self.assertEqual(0, len(node.peer_relays))
        self.assertEqual(0, len(node.peer_transaction_relays))

        node.enqueue_disconnect.assert_called_once_with(relay_transaction_conn.fileno)

    def test_split_relay_no_reconnect_disconnect_transaction(self):
        sdn_http_service.submit_peer_connection_error_event = MagicMock()
        node = initialize_split_relay_node()
        node.num_retries_by_ip[(LOCALHOST, 8002)] = constants.MAX_CONNECT_RETRIES

        relay_block_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8001)
        relay_transaction_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8002)
        self.assertEqual(True, node.should_retry_connection(LOCALHOST, 8001, relay_block_conn.CONNECTION_TYPE))
        self.assertEqual(False, node.should_retry_connection(LOCALHOST, 8002, relay_transaction_conn.CONNECTION_TYPE))

        node.on_connection_closed(relay_transaction_conn.fileno)
        self.assertEqual(1, len(node.alarm_queue.alarms))
        self.assertEqual(node._retry_init_client_socket, node.alarm_queue.alarms[0].alarm.fn)
        node.enqueue_disconnect.assert_called_once_with(relay_transaction_conn.fileno)
        node.enqueue_disconnect.reset_mock()

        time.time = MagicMock(return_value=time.time() + constants.CONNECTION_RETRY_SECONDS)
        node.alarm_queue.fire_alarms()

        node.enqueue_connection.assert_not_called()
        sdn_http_service.submit_peer_connection_error_event.assert_has_calls(
            [
                call(node.opts.node_id, LOCALHOST, 8001),
                call(node.opts.node_id, LOCALHOST, 8002),
            ],
            any_order=True
        )
        self.assertEqual(0, len(node.peer_relays))
        self.assertEqual(0, len(node.peer_transaction_relays))

        node.enqueue_disconnect.assert_called_once_with(relay_block_conn.fileno)
