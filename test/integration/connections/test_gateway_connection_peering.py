import asyncio
from typing import Callable

from bxcommon.models.node_type import NodeType
from mock import MagicMock

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import LOCALHOST
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network.node_event_loop import NodeEventLoop
from bxcommon.services import sdn_http_service
from bxcommon.test_utils import helpers, integration_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.testing.null_gateway_node import NullGatewayNode


def run_test(main_event_loop: NodeEventLoop, peer_event_loop: NodeEventLoop, test_function: Callable) -> None:

    async def run_main_event_loop():
        await main_event_loop.run()

    async def run_test_async():
        await peer_event_loop.wait_started()
        await main_event_loop.wait_started()
        test_function()
        main_event_loop.stop()
        peer_event_loop.stop()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(peer_event_loop.run(), run_main_event_loop(), run_test_async())
    )


class GatewayConnectionPeeringTest(AbstractTestCase):
    def setUp(self):
        self.main_port = helpers.get_free_port()
        self.peer_port = helpers.get_free_port()

        sdn_http_service.fetch_gateway_peers = MagicMock(return_value=[])

        self.main_opts = helpers.get_gateway_opts(
            self.main_port, node_id="main",
            peer_gateways=[OutboundPeerModel(LOCALHOST, self.peer_port, node_type=NodeType.EXTERNAL_GATEWAY)]
        )
        self.main_gateway = NullGatewayNode(self.main_opts)
        self.main_event_loop = NodeEventLoop(self.main_gateway)

        self.peer_opts = helpers.get_gateway_opts(self.peer_port, node_id="peer")
        self.peer_gateway = NullGatewayNode(self.peer_opts)
        self.peer_event_loop = NodeEventLoop(self.peer_gateway)

    def test_gateway_to_gateway_connection_initialization_assign_port_from_hello(self):
        run_test(
            self.main_event_loop,
            self.peer_event_loop,
            self._test_gateway_to_gateway_connection_initialization_assign_port_from_hello
        )

    def test_duplicate_gateway_connections(self):
        run_test(self.main_event_loop, self.peer_event_loop, self._test_duplicate_gateway_connections)

    def _test_gateway_to_gateway_connection_initialization_assign_port_from_hello(self):
        self.assertEqual(1, len(self.main_gateway.connection_pool))
        self.assertTrue(self.main_gateway.connection_exists(LOCALHOST, self.peer_port))
        gateway_connection = self.main_gateway.connection_pool.get_by_ipport(LOCALHOST, self.peer_port)
        self.assertEqual(GatewayConnection, type(gateway_connection))

        # send hello message
        integration_helpers.send_on_connection(gateway_connection)

        self.assertTrue(gateway_connection.is_active())
        self.assertTrue(self.peer_gateway.connection_exists(LOCALHOST, self.main_port))
        self.assertTrue(self.peer_gateway.connection_pool.get_by_ipport(LOCALHOST, self.main_port).state &
                        ConnectionState.ESTABLISHED)

    def _test_duplicate_gateway_connections(self):
        self.assertEqual(1, len(self.main_gateway.connection_pool))
        self.assertEqual(1, len(self.peer_gateway.connection_pool))
        main_connection = next(iter(self.main_gateway.connection_pool.by_ipport.values()))
        peer_connection = next(iter(self.peer_gateway.connection_pool.by_ipport.values()))
        self.assertNotEqual(main_connection.direction, peer_connection.direction)
        self.assertEqual(main_connection.ordering, peer_connection.ordering)
