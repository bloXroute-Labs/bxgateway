import time

from mock import MagicMock, call

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import LOCALHOST, CONNECTION_RETRY_SECONDS, MAX_CONNECT_RETRIES, SDN_CONTACT_RETRY_SECONDS
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.services import sdn_http_service
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayNode(AbstractGatewayNode):

    def get_remote_blockchain_connection_cls(self):
        pass

    def get_blockchain_connection_cls(self):
        pass

    def get_relay_connection_cls(self):
        pass


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
        node.num_retries_by_ip[LOCALHOST] = MAX_CONNECT_RETRIES
        node._connection_timeout(cli_peer_conn)

        node.alarm_queue.register_alarm.assert_has_calls([call(CONNECTION_RETRY_SECONDS, node._retry_init_client_socket,
                                                               LOCALHOST, 8001, ConnectionType.GATEWAY)])
        node._retry_init_client_socket(LOCALHOST, 8001, ConnectionType.GATEWAY)
        self.assertEqual(MAX_CONNECT_RETRIES + 1, node.num_retries_by_ip[LOCALHOST])

    def test_gateway_peer_get_more_peers_when_too_few_gateways(self):
        peer_gateways = [
            OutboundPeerModel(LOCALHOST, 8001)
        ]
        node = GatewayNode(helpers.get_gateway_opts(8000, peer_gateways=peer_gateways, min_peer_gateways=2))
        node.peer_gateways.add(OutboundPeerModel(LOCALHOST, 8002))

        sdn_http_service.fetch_gateway_peers = MagicMock(return_value=[
            OutboundPeerModel(LOCALHOST, 8003)
        ])

        node.on_connection_added(MockSocketConnection(), LOCALHOST, 8002, True)
        not_cli_peer_conn = node.connection_pool.get_by_ipport(LOCALHOST, 8002)
        node.destroy_conn(not_cli_peer_conn)

        time.time = MagicMock(return_value=time.time() + SDN_CONTACT_RETRY_SECONDS * 2)
        node.alarm_queue.fire_alarms()

        sdn_http_service.fetch_gateway_peers.assert_has_calls([call(node.opts.node_id), call(node.opts.node_id)])
        self.assertEqual(2, len(node.outbound_peers))
        self.assertIn(OutboundPeerModel(LOCALHOST, 8001), node.outbound_peers)
        self.assertIn(OutboundPeerModel(LOCALHOST, 8003), node.outbound_peers)

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

