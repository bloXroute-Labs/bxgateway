from contextlib import closing

from mock import MagicMock

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import LOCALHOST
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network import network_event_loop_factory
from bxcommon.services import sdn_http_service
from bxcommon.test_utils import helpers, integration_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.network_thread import NetworkThread
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.testing.null_gateway_node import NullGatewayNode


# noinspection PyProtectedMember
def reinit_gateway_connection_with_ordering(connection, ordering):
    connection.outputbuf._flush_to_buffer()
    connection.outputbuf.advance_buffer(connection.outputbuf.length)
    connection.ordering = ordering
    connection.enqueue_msg(GatewayHelloMessage(connection.protocol_version, connection.network_num,
                                               connection.node.opts.external_ip, connection.node.opts.external_port,
                                               connection.ordering))


class GatewayNodeTest(AbstractTestCase):
    def setUp(self):
        self.main_port = helpers.get_free_port()
        self.peer_port = helpers.get_free_port()

        sdn_http_service.fetch_gateway_peers = MagicMock(return_value=[])
        sdn_http_service.fetch_relay_peers = MagicMock(return_value=[])

        self.main_opts = helpers.get_gateway_opts(self.main_port, node_id="main",
                                                  peer_gateways=[OutboundPeerModel(LOCALHOST, self.peer_port)])
        self.main_gateway = NullGatewayNode(self.main_opts)
        self.main_event_loop = network_event_loop_factory.create_event_loop(self.main_gateway)

        self.peer_opts = helpers.get_gateway_opts(self.peer_port, node_id="peer")
        self.peer_gateway = NullGatewayNode(self.peer_opts)
        self.peer_thread = NetworkThread(self.peer_gateway)

    def test_gateway_to_gateway_connection_initialization_assign_port_from_hello(self):
        with closing(self.peer_thread):
            self.main_event_loop._start_server()
            self.peer_thread.start()

            # wait for peer server start
            integration_helpers.wait_for_a_connection(self.peer_thread.event_loop, 0)
            self.main_event_loop._connect_to_peers()

            self.assertEqual(1, len(self.main_gateway.connection_pool))
            self.assertTrue(self.main_gateway.connection_exists(LOCALHOST, self.peer_port))
            gateway_connection = self.main_gateway.connection_pool.get_byipport(LOCALHOST, self.peer_port)
            self.assertEqual(GatewayConnection, type(gateway_connection))

            # send hello message
            integration_helpers.send_on_connection(gateway_connection)

            # receive ack
            integration_helpers.receive_on_connection(gateway_connection)

            self.assertTrue(gateway_connection.state & ConnectionState.ESTABLISHED)
            self.assertTrue(self.peer_gateway.connection_exists(LOCALHOST, self.main_port))
            self.assertTrue(self.peer_gateway.connection_pool.get_byipport(LOCALHOST, self.main_port).state &
                            ConnectionState.ESTABLISHED)

    def _test_gateway_to_gateway_connection_resolution(self, test_fn):
        self.peer_opts = helpers.get_gateway_opts(self.peer_port, node_id="peer",
                                                  peer_gateways=[OutboundPeerModel(LOCALHOST, self.main_port)])
        self.peer_gateway = NullGatewayNode(self.peer_opts)
        self.peer_thread = NetworkThread(self.peer_gateway)

        with closing(self.peer_thread):
            self.main_event_loop._start_server()
            self.peer_thread.start()

            # wait for peer server start
            integration_helpers.wait_for_a_connection(self.peer_thread.event_loop, 0)
            self.main_event_loop._connect_to_peers()

            # wait for connection events to be processed
            integration_helpers.wait_for_a_connection(self.main_event_loop, 1)
            integration_helpers.wait_for_a_connection(self.peer_thread.event_loop, 1)

            self.assertEqual(2, len(self.main_event_loop._socket_connections))
            server_socket = integration_helpers.get_server_socket(self.main_event_loop)
            integration_helpers.accept_a_connection(self.main_event_loop, server_socket)

            self.assertEqual(2, len(self.main_gateway.connection_pool))
            peer_connected_port = filter(lambda address: address[1] != self.peer_port,
                                         self.main_gateway.connection_pool.byipport.keys())[0][1]
            peer_initiated_connection = self.main_gateway.connection_pool.get_byipport(LOCALHOST, peer_connected_port)
            main_initiated_connection = self.main_gateway.connection_pool.get_byipport(LOCALHOST, self.peer_port)

            # find peer initiated connection so we can get the ordering
            integration_helpers.wait_while(
                lambda: len(self.peer_gateway.connection_pool.byipport) < 2
            )

            peer_initiated_connection_on_peer_key = filter(lambda address: address[1] == self.main_port,
                                                           self.peer_gateway.connection_pool.byipport.keys())[0]
            peer_initiated_connection_on_peer = self.peer_gateway.connection_pool.byipport[
                peer_initiated_connection_on_peer_key
            ]
            main_initiated_connection_on_peer_key = filter(lambda address: address[1] != self.main_port,
                                                           self.peer_gateway.connection_pool.byipport.keys())[0]
            main_initiated_connection_on_peer = self.peer_gateway.connection_pool.byipport[
                main_initiated_connection_on_peer_key
            ]
            integration_helpers.wait_while(
                lambda: peer_initiated_connection_on_peer.ordering == GatewayConnection.NULL_ORDERING
            )
            test_fn(main_initiated_connection, peer_initiated_connection,
                    main_initiated_connection_on_peer, peer_initiated_connection_on_peer)

    def test_gateway_to_gateway_connection_resolve_duplicate_same_ordering_retry(self):
        def peer_initiated_equal_then_higher(main_initiated_connection, peer_initiated_connection,
                                             main_initiated_connection_on_peer, peer_initiated_connection_on_peer):
            original_ordering = peer_initiated_connection_on_peer.ordering
            reinit_gateway_connection_with_ordering(main_initiated_connection,
                                                    peer_initiated_connection_on_peer.ordering)

            # receive hello message
            integration_helpers.receive_on_connection(peer_initiated_connection)

            # send hello message
            integration_helpers.send_on_connection(main_initiated_connection)

            integration_helpers.wait_while(
                lambda: (main_initiated_connection_on_peer.ordering == GatewayConnection.NULL_ORDERING or
                         main_initiated_connection_on_peer.ordering == main_initiated_connection.ordering)
            )

            new_ordering = main_initiated_connection_on_peer.ordering
            reinit_gateway_connection_with_ordering(peer_initiated_connection, new_ordering + 1)
            # receive hello message again, now with lower value
            integration_helpers.receive_on_connection(main_initiated_connection)

            # send hello message again, now with higher value
            integration_helpers.send_on_connection(peer_initiated_connection)

            # receive hello message again, now with lower value
            integration_helpers.receive_on_connection(main_initiated_connection)

            # wait for peer to process hello message
            integration_helpers.wait_while(
                lambda: (peer_initiated_connection_on_peer.ordering == original_ordering and
                         main_initiated_connection_on_peer.ordering == new_ordering)
            )

            # assert on results
            self.assertTrue(main_initiated_connection.state & ConnectionState.MARK_FOR_CLOSE)
            self.assertTrue(main_initiated_connection_on_peer.state & ConnectionState.MARK_FOR_CLOSE)
            self.assertTrue(peer_initiated_connection.state & ConnectionState.ESTABLISHED)
            self.assertTrue(peer_initiated_connection_on_peer.state & ConnectionState.ESTABLISHED)

            # connection is cleaned up from byipaddr but not byfileno
            self.assertEqual(1, len(self.main_gateway.connection_pool))
            self.assertEqual(2, len(list(self.main_gateway.connection_pool.items())))

            # peer thread should cleanup completely
            integration_helpers.wait_while(lambda: len(self.peer_gateway.connection_pool) > 1)

            self.assertEquals(peer_initiated_connection,
                              self.main_gateway.connection_pool.get_byipport(LOCALHOST, self.peer_port))
            self.assertEquals(peer_initiated_connection_on_peer,
                              self.peer_gateway.connection_pool.get_byipport(LOCALHOST, self.main_port))

        self._test_gateway_to_gateway_connection_resolution(peer_initiated_equal_then_higher)

    def test_gateway_to_gateway_connection_resolve_duplicate_peer_initiated_higher(self):
        def peer_initiated_higher(main_initiated_connection, peer_initiated_connection,
                                  main_initiated_connection_on_peer, peer_initiated_connection_on_peer):
            reinit_gateway_connection_with_ordering(main_initiated_connection,
                                                    peer_initiated_connection_on_peer.ordering - 1)

            # send hello message
            integration_helpers.send_on_connection(main_initiated_connection)

            # receive hello message
            integration_helpers.receive_on_connection(peer_initiated_connection)

            # send ack/etc.
            integration_helpers.send_on_connection(peer_initiated_connection)

            # receive ack/close/etc.
            integration_helpers.receive_on_connection(main_initiated_connection)

            # assert on results
            self.assertTrue(main_initiated_connection.state & ConnectionState.MARK_FOR_CLOSE)
            self.assertTrue(main_initiated_connection_on_peer.state & ConnectionState.MARK_FOR_CLOSE)
            self.assertTrue(peer_initiated_connection.state & ConnectionState.ESTABLISHED)
            self.assertTrue(peer_initiated_connection_on_peer.state & ConnectionState.ESTABLISHED)

            # connection is cleaned up from byipaddr but not byfileno
            self.assertEqual(1, len(self.main_gateway.connection_pool))
            self.assertEqual(2, len(list(self.main_gateway.connection_pool.items())))

            # peer thread will cleanup dead connection later
            integration_helpers.wait_while(lambda: len(self.peer_gateway.connection_pool) > 2)

            self.assertEquals(peer_initiated_connection,
                              self.main_gateway.connection_pool.get_byipport(LOCALHOST, self.peer_port))
            self.assertEquals(peer_initiated_connection_on_peer,
                              self.peer_gateway.connection_pool.get_byipport(LOCALHOST, self.main_port))

        self._test_gateway_to_gateway_connection_resolution(peer_initiated_higher)

    def test_gateway_to_gateway_connection_resolve_duplicate_connections_peer_initiated_lower(self):
        def peer_initiated_lower(main_initiated_connection, peer_initiated_connection,
                                 main_initiated_connection_on_peer, peer_initiated_connection_on_peer):
            reinit_gateway_connection_with_ordering(main_initiated_connection,
                                                    peer_initiated_connection_on_peer.ordering + 1)

            # receive hello message
            integration_helpers.receive_on_connection(peer_initiated_connection)

            # send hello message
            integration_helpers.send_on_connection(main_initiated_connection)

            # send ack/close/etc.
            integration_helpers.send_on_connection(peer_initiated_connection)

            # receive ack/close/etc.
            integration_helpers.receive_on_connection(main_initiated_connection)
            integration_helpers.receive_on_connection(peer_initiated_connection)

            # assert on results
            self.assertTrue(main_initiated_connection.state & ConnectionState.ESTABLISHED)
            self.assertTrue(main_initiated_connection_on_peer.state & ConnectionState.ESTABLISHED)
            self.assertTrue(peer_initiated_connection.state & ConnectionState.MARK_FOR_CLOSE)
            self.assertTrue(peer_initiated_connection_on_peer.state & ConnectionState.MARK_FOR_CLOSE)

            # connection is not cleaned up yet
            self.assertEqual(2, len(self.main_gateway.connection_pool))
            self.assertEqual(2, len(list(self.main_gateway.connection_pool.items())))

            # peer thread should cleanup completely
            integration_helpers.wait_while(lambda: len(self.peer_gateway.connection_pool) > 1)

            self.assertEquals(main_initiated_connection,
                              self.main_gateway.connection_pool.get_byipport(LOCALHOST, self.peer_port))
            self.assertEquals(main_initiated_connection_on_peer,
                              self.peer_gateway.connection_pool.get_byipport(LOCALHOST, self.main_port))

        self._test_gateway_to_gateway_connection_resolution(peer_initiated_lower)
