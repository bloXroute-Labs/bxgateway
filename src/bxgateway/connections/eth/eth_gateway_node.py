from bxcommon.connections.node_type import NodeType
from bxcommon.network.transport_layer_protocol import TransportLayerProtocol
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_node_discovery_connection import EthNodeDiscoveryConnection
from bxgateway.connections.eth.eth_relay_connection import EthRelayConnection
from bxgateway.utils.eth import rlp_utils, crypto_utils


class EthGatewayNode(AbstractGatewayNode):
    def __init__(self, opts):
        super(EthGatewayNode, self).__init__(opts)

        self._remote_public_key = None

    def get_blockchain_connection_cls(self):
        return EthNodeDiscoveryConnection if self._is_in_discovery() else EthNodeConnection

    def get_relay_connection_cls(self):
        return EthRelayConnection

    def get_outbound_peer_addresses(self):
        peers = []

        for peer in self.opts.outbound_peers:
            peers.append((peer.ip, peer.port))

        protocol = TransportLayerProtocol.UDP if self._is_in_discovery() else TransportLayerProtocol.TCP
        peers.append((self.opts.blockchain_ip, self.opts.blockchain_port, protocol))

        return peers

    def get_private_key(self):
        return rlp_utils.decode_hex(self.opts.private_key)

    def get_public_key(self):
        return crypto_utils.private_to_public_key(self.get_private_key())

    def set_remote_public_key(self, discovery_connection, remote_public_key):
        if not isinstance(discovery_connection, EthNodeDiscoveryConnection):
            raise TypeError("Argument discovery_connection is expected to be of type EthNodeDiscoveryConnection, was {}"
                            .format(type(discovery_connection)))

        if not remote_public_key:
            raise ValueError("remote_public_key argument is required")

        self._remote_public_key = remote_public_key

        # close UDP connection
        self.enqueue_disconnect(discovery_connection.socket_connection.fileno())

        self.connection_pool.delete(discovery_connection)

        # establish TCP connection
        self.enqueue_connection(self.opts.blockchain_ip, self.opts.blockchain_port)

    def get_remote_public_key(self):
        return self._remote_public_key

    def _is_in_discovery(self):
        return not self.opts.no_discovery and self._remote_public_key is None
