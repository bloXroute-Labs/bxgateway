from argparse import Namespace

from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network.transport_layer_protocol import TransportLayerProtocol
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway import eth_constants
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_node_discovery_connection import EthNodeDiscoveryConnection
from bxgateway.utils.eth import crypto_utils


class ETHGatewayNodeTest(AbstractTestCase):

    def test_get_outbound_peer_addresses__initiate_handshake(self):
        self._test_get_outbound_peer_addresses(True, TransportLayerProtocol.UDP)

    def test_get_outbound_peer_addresses__no_initiate_handshake(self):
        self._test_get_outbound_peer_addresses(False, TransportLayerProtocol.TCP)

    def test_get_gateway_connection_class__do_not_initiate_handshake(self):
        node = self._set_up_test_node(False)
        connection_cls = node._get_gateway_connection_cls()
        self.assertEqual(connection_cls, EthNodeConnection)

    def test_get_gateway_connection_class__initiate_handshake_no_remote_pub_key(self):
        node = self._set_up_test_node(True)
        connection_cls = node.get_connection_class(self.blockchain_ip, self.blockchain_port)
        self.assertEqual(connection_cls, EthNodeDiscoveryConnection)

    def test_get_gateway_connection_class__initiate_handshake_with_remote_pub_key(self):
        dummy_con_fileno = 123
        dummy_con_ip = "0.0.0.0"
        dummy_con_port = 12345
        node = self._set_up_test_node(True)
        remote_public_key = self._get_dummy_public_key()
        discovery_connection = EthNodeDiscoveryConnection(MockSocketConnection(dummy_con_fileno),
                                                          (dummy_con_ip, dummy_con_port),
                                                          node,
                                                          False)
        node.connection_pool.add(dummy_con_fileno, dummy_con_ip, dummy_con_port, discovery_connection)
        node.set_remote_public_key(discovery_connection, remote_public_key)
        connection_cls = node.get_connection_class(self.blockchain_ip, self.blockchain_port)
        self.assertEqual(connection_cls, EthNodeConnection)

    def test_get_private_key(self):
        node = self._set_up_test_node(False)
        private_key = node.get_private_key()
        self.assertTrue(private_key)
        self.assertEqual(len(private_key), eth_constants.PRIVATE_KEY_LEN)

    def test_get_public_key(self):
        node = self._set_up_test_node(False)
        public_key = node.get_public_key()
        self.assertTrue(public_key)
        self.assertEqual(len(public_key), eth_constants.PUBLIC_KEY_LEN)

    def test_get_remote_public_key__default(self):
        node = self._set_up_test_node(False)
        remote_public_key = node.get_remote_public_key()
        self.assertIsNone(remote_public_key)

    def test_set_remote_public_key(self):
        dummy_con_fileno = 123
        dummy_con_ip = "0.0.0.0"
        dummy_con_port = 12345
        node = self._set_up_test_node(False)
        discovery_connection = EthNodeDiscoveryConnection(MockSocketConnection(dummy_con_fileno),
                                                          (dummy_con_ip, dummy_con_port),
                                                          node,
                                                          False)
        node.connection_pool.add(dummy_con_fileno, dummy_con_ip, dummy_con_port, discovery_connection)
        self.assertEqual(1, len(self.node.connection_pool))

        remote_public_key = node.get_remote_public_key()
        self.assertIsNone(remote_public_key)

        new_remote_public_key = self._get_dummy_public_key()
        node.set_remote_public_key(discovery_connection, new_remote_public_key)

        self.assertEqual(0, len(self.node.connection_pool))
        self.assertEqual(1, len(self.node.disconnect_queue))
        self.assertEqual(dummy_con_fileno, self.node.disconnect_queue.pop())

        updated_remote_public_key = node.get_remote_public_key()
        self.assertIsNotNone(updated_remote_public_key)

    def _test_get_outbound_peer_addresses(self, initiate_handshake, expected_node_con_protocol):
        node = self._set_up_test_node(initiate_handshake)
        assert isinstance(node, EthGatewayNode)

        peer_addresses = node.get_outbound_peer_addresses()
        self.assertTrue(peer_addresses)

        self.assertEqual(len(self.servers) + 1, len(peer_addresses))

        server_index = 0
        for server in self.servers:
            self.assertEqual(server.ip, peer_addresses[server_index][0])
            self.assertEqual(server.port, peer_addresses[server_index][1])

            server_index += 1

        self.assertEqual(self.blockchain_ip, peer_addresses[len(self.servers)][0])
        self.assertEqual(self.blockchain_port, peer_addresses[len(self.servers)][1])
        self.assertEqual(expected_node_con_protocol, peer_addresses[len(self.servers)][2])

    def _set_up_test_node(self, initialize_handshake):
        # Dummy address
        self.server_ip = "127.0.0.1"
        self.server_port = 1234

        self.blockchain_ip = "0.0.0.0"
        self.blockchain_port = "30303"


        # Setting up dummy server addresses
        self.servers = [
            OutboundPeerModel("172.0.0.1", 2222),
            OutboundPeerModel("172.0.0.2", 3333),
            OutboundPeerModel("172.0.0.3", 4444)
        ]

        opts = Namespace()
        opts.__dict__ = {
            "sid_expire_time": 0,
            "external_ip": self.server_ip,
            "external_port": self.server_port,
            "test_mode": [],
            "outbound_peers": self.servers,

            "blockchain_ip": self.blockchain_ip,
            "blockchain_port": self.blockchain_port,

            "private_key": "294549f8629f0eeb2b8e01aca491f701f5386a9662403b485c4efe7d447dfba3",
            "network_id": 1,
            "chain_difficulty": 4194304,
            "genesis_hash": "1e8ff5fd9d06ab673db775cf5c72a6b2d63171cd26fe1e6a8b9d2d696049c781",
            "no_discovery": not initialize_handshake
        }

        self.node = EthGatewayNode(opts)
        self.assertTrue(self.node)

        return self.node

    def _get_dummy_public_key(self):
        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        return crypto_utils.private_to_public_key(dummy_private_key)
