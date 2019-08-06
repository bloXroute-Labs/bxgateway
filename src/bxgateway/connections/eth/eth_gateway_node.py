from typing import Tuple

from bxcommon import constants
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.network.transport_layer_protocol import TransportLayerProtocol
from bxcommon.utils import convert, logger
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_node_discovery_connection import EthNodeDiscoveryConnection
from bxgateway.connections.eth.eth_relay_connection import EthRelayConnection
from bxgateway.connections.eth.eth_remote_connection import EthRemoteConnection
from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.testing.eth_lossy_relay_connection import EthLossyRelayConnection
from bxgateway.testing.test_modes import TestModes
from bxgateway.utils.eth import crypto_utils


class EthGatewayNode(AbstractGatewayNode):
    def __init__(self, opts):
        super(EthGatewayNode, self).__init__(opts)

        self._node_public_key = None
        self._remote_public_key = None

        if opts.node_public_key is not None:
            self._node_public_key = convert.hex_to_bytes(opts.node_public_key)

        if opts.remote_blockchain_peer is not None:
            if opts.remote_public_key is None:
                raise ValueError("Public key must be included with command-line specified remote blockchain peer.")
            else:
                self._remote_public_key = convert.hex_to_bytes(opts.remote_public_key)

    def build_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                    from_me: bool) -> AbstractGatewayBlockchainConnection:
        if self._is_in_local_discovery():
            return EthNodeDiscoveryConnection(socket_connection, address, self, from_me)
        else:
            return EthNodeConnection(socket_connection, address, self, from_me)

    def build_relay_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                               from_me: bool) -> AbstractRelayConnection:
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            cls = EthLossyRelayConnection
        else:
            cls = EthRelayConnection

        relay_connection = cls(socket_connection, address, self, from_me)
        return relay_connection

    def build_remote_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                           from_me: bool) -> AbstractGatewayBlockchainConnection:
        return EthRemoteConnection(socket_connection, address, self, from_me)

    def build_block_queuing_service(self) -> BlockQueuingService:
        return EthBlockQueuingService(self)

    def on_updated_remote_blockchain_peer(self, peer):
        if "node_public_key" not in peer.attributes:
            logger.warn("Received remote blockchain peer without remote public key. This is currently unsupported.")
            return constants.SDN_CONTACT_RETRY_SECONDS
        else:
            super(EthGatewayNode, self).on_updated_remote_blockchain_peer(peer)
            self._remote_public_key = convert.hex_to_bytes(peer.attributes["node_public_key"])
            return constants.CANCEL_ALARMS

    def get_outbound_peer_addresses(self):
        peers = []

        for peer in self.opts.outbound_peers:
            peers.append((peer.ip, peer.port))

        local_protocol = TransportLayerProtocol.UDP if self._is_in_local_discovery() else TransportLayerProtocol.TCP
        peers.append((self.opts.blockchain_ip, self.opts.blockchain_port, local_protocol))

        if self.remote_blockchain_ip is not None and self.remote_blockchain_port is not None:
            remote_protocol = TransportLayerProtocol.UDP if self._is_in_remote_discovery() else \
                TransportLayerProtocol.TCP
            peers.append((self.remote_blockchain_ip, self.remote_blockchain_port, remote_protocol))

        return peers

    def get_private_key(self):
        return convert.hex_to_bytes(self.opts.private_key)

    def get_public_key(self):
        return crypto_utils.private_to_public_key(self.get_private_key())

    def set_node_public_key(self, discovery_connection, node_public_key):
        if not isinstance(discovery_connection, EthNodeDiscoveryConnection):
            raise TypeError("Argument discovery_connection is expected to be of type EthNodeDiscoveryConnection, was {}"
                            .format(type(discovery_connection)))

        if not node_public_key:
            raise ValueError("node_public_key argument is required")

        self._node_public_key = node_public_key

        # close UDP connection
        self.enqueue_disconnect(discovery_connection.socket_connection.fileno())

        self.connection_pool.delete(discovery_connection)

        # establish TCP connection
        self.enqueue_connection(self.opts.blockchain_ip, self.opts.blockchain_port)

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
        self.enqueue_connection(self.remote_blockchain_ip, self.remote_blockchain_port)

    def get_node_public_key(self):
        return self._node_public_key

    def get_remote_public_key(self):
        return self._remote_public_key

    def _is_in_local_discovery(self):
        return not self.opts.no_discovery and self._node_public_key is None

    def _is_in_remote_discovery(self):
        return not self.opts.no_discovery and self._remote_public_key is None
