from collections import deque
from typing import Optional, List, Deque, cast

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.network.peer_info import ConnectionPeerInfo
from bxcommon.network.transport_layer_protocol import TransportLayerProtocol
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import eth_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_node_discovery_connection import EthNodeDiscoveryConnection
from bxgateway.connections.eth.eth_relay_connection import EthRelayConnection
from bxgateway.connections.eth.eth_remote_connection import EthRemoteConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.eth.eth_block_processing_service import EthBlockProcessingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.services.eth.eth_normal_block_cleanup_service import EthNormalBlockCleanupService
from bxgateway.services.push_block_queuing_service import PushBlockQueuingService
from bxgateway.testing.eth_lossy_relay_connection import EthLossyRelayConnection
from bxgateway.testing.test_modes import TestModes
from bxgateway.utils.eth import crypto_utils
from bxgateway.utils.stats.eth.eth_gateway_stats_service import eth_gateway_stats_service
from bxutils import logging
from bxgateway import log_messages, eth_constants
from bxutils.services.node_ssl_service import NodeSSLService

logger = logging.get_logger(__name__)


class EthGatewayNode(AbstractGatewayNode):
    DISCONNECT_REASON_TO_DESCRIPTION = {
        0: "Disconnect requested",
        1: "TCP sub-system error",
        2: "Breach of protocol, e.g. a malformed message or bad RLP",
        3: "Useless peer",
        4: "Too many peers",
        5: "Already connected",
        6: "Incompatible P2P protocol version",
        7: "Null node identity received - this is automatically invalid",
        8: "Client quitting",
        9: "Unexpected identity in handshake",
        10: "Identity is the same as this node",
        11: "Ping timeout",
        16: "Some other reason specific to a subprotocol"
    }

    DISCONNECT_REASON_TO_INSTRUCTION = {
        4: "Remove some peers or add gateway as trusted peer.",
        5: "Verify that another instance of the gateway is not already running.",
        16: "Verify that '--blockchain-network' and other blockchain network parameters are correct."
    }

    def __init__(self, opts, node_ssl_service: NodeSSLService):
        super(EthGatewayNode, self).__init__(opts, node_ssl_service, eth_constants.TRACKED_BLOCK_CLEANUP_INTERVAL_S)

        self._node_public_key = None
        self._remote_public_key = None

        if opts.node_public_key is not None:
            self._node_public_key = convert.hex_to_bytes(opts.node_public_key)
        else:
            raise RuntimeError(
                "128 digit public key must be included with command-line specified blockchain peer.")
        if opts.remote_blockchain_peer is not None:
            if opts.remote_public_key is None:
                raise RuntimeError(
                    "128 digit public key must be included with command-line specified remote blockchain peer.")
            else:
                self._remote_public_key = convert.hex_to_bytes(opts.remote_public_key)

        self.block_processing_service: EthBlockProcessingService = EthBlockProcessingService(self)
        self.block_queuing_service: EthBlockQueuingService = EthBlockQueuingService(self)

        # List of know total difficulties, tuples of values (block hash, total difficulty)
        self._last_known_difficulties = deque(maxlen=eth_constants.LAST_KNOWN_TOTAL_DIFFICULTIES_MAX_COUNT)

        # queue of the block hashes requested from remote blockchain node during sync
        self._requested_remote_blocks_queue = deque()

        # number of remote block requests to skip in case if requests and responses got out of sync
        self._skip_remote_block_requests_stats_count = 0

        self.init_eth_gateway_stat_logging()

        self.message_converter = EthMessageConverter()

        logger.info("Gateway enode url: {}", self.get_enode())

    def build_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        if self._is_in_local_discovery():
            return EthNodeDiscoveryConnection(socket_connection, self)
        else:
            return EthNodeConnection(socket_connection, self)

    def build_relay_connection(self, socket_connection: AbstractSocketConnectionProtocol) -> AbstractRelayConnection:
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            cls = EthLossyRelayConnection
        else:
            cls = EthRelayConnection

        relay_connection = cls(socket_connection, self)
        return relay_connection

    def build_remote_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        return EthRemoteConnection(socket_connection, self)

    def build_block_queuing_service(self) -> PushBlockQueuingService:
        return EthBlockQueuingService(self)

    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        if self.opts.use_extensions:
            from bxgateway.services.eth.eth_extension_block_cleanup_service import EthExtensionBlockCleanupService
            return EthExtensionBlockCleanupService(self, self.network_num)
        else:
            return EthNormalBlockCleanupService(self, self.network_num)

    def on_updated_remote_blockchain_peer(self, peer):
        if "node_public_key" not in peer.attributes:
            logger.warning(log_messages.BLOCKCHAIN_PEER_LACKS_PUBLIC_KEY)
            return constants.SDN_CONTACT_RETRY_SECONDS
        else:
            super(EthGatewayNode, self).on_updated_remote_blockchain_peer(peer)
            self._remote_public_key = convert.hex_to_bytes(peer.attributes["node_public_key"])
            return constants.CANCEL_ALARMS

    def get_outbound_peer_info(self) -> List[ConnectionPeerInfo]:
        peers = []

        for peer in self.opts.outbound_peers:
            peers.append(ConnectionPeerInfo(
                IpEndpoint(peer.ip, peer.port), convert.peer_node_to_connection_type(self.NODE_TYPE, peer.node_type)
            ))

        local_protocol = TransportLayerProtocol.UDP if self._is_in_local_discovery() else TransportLayerProtocol.TCP
        peers.append(ConnectionPeerInfo(
            IpEndpoint(self.opts.blockchain_ip, self.opts.blockchain_port),
            ConnectionType.BLOCKCHAIN_NODE,
            local_protocol
        ))

        if self.remote_blockchain_ip is not None and self.remote_blockchain_port is not None:
            remote_protocol = TransportLayerProtocol.UDP if self._is_in_remote_discovery() else \
                TransportLayerProtocol.TCP
            peers.append(ConnectionPeerInfo(
                # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
                IpEndpoint(self.remote_blockchain_ip, self.remote_blockchain_port),
                ConnectionType.REMOTE_BLOCKCHAIN_NODE,
                remote_protocol
            ))

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
        discovery_connection.mark_for_close(False)

        # establish TCP connection
        self.enqueue_connection(self.opts.blockchain_ip, self.opts.blockchain_port, ConnectionType.BLOCKCHAIN_NODE)

    def set_remote_public_key(self, discovery_connection, remote_public_key):
        if not isinstance(discovery_connection, EthNodeDiscoveryConnection):
            raise TypeError("Argument discovery_connection is expected to be of type EthNodeDiscoveryConnection, was {}"
                            .format(type(discovery_connection)))

        if not remote_public_key:
            raise ValueError("remote_public_key argument is required")

        self._remote_public_key = remote_public_key

        # close UDP connection
        discovery_connection.mark_for_close(False)

        # establish TCP connection
        self.enqueue_connection(
            self.remote_blockchain_ip, self.remote_blockchain_port, ConnectionType.REMOTE_BLOCKCHAIN_NODE
        )

    def get_node_public_key(self):
        return self._node_public_key

    def get_remote_public_key(self):
        return self._remote_public_key

    def set_known_total_difficulty(self, block_hash: Sha256Hash, total_difficulty: int) -> None:
        self._last_known_difficulties.append((block_hash, total_difficulty))

    def try_calculate_total_difficulty(self, block_hash: Sha256Hash, new_block_parts: NewBlockParts) -> Optional[int]:
        previous_block_hash = new_block_parts.get_previous_block_hash()
        previous_block_total_difficulty = None

        for known_block_hash, known_total_difficulty in self._last_known_difficulties:
            if previous_block_hash == known_block_hash:
                previous_block_total_difficulty = known_total_difficulty
                break

        if previous_block_total_difficulty is None:
            logger.debug("Unable to calculate total difficulty after block {}.",
                         convert.bytes_to_hex(block_hash.binary))
            return None

        block_total_difficulty = previous_block_total_difficulty + new_block_parts.get_block_difficulty()

        self._last_known_difficulties.append((block_hash, block_total_difficulty))
        logger.debug("Calculated total difficulty after block {} = {}.",
                     convert.bytes_to_hex(block_hash.binary), block_total_difficulty)

        return block_total_difficulty

    def log_requested_remote_blocks(self, block_hashes: List[Sha256Hash]) -> None:
        if self._skip_remote_block_requests_stats_count > 0:
            self._skip_remote_block_requests_stats_count -= 1
        else:
            for block_hash in block_hashes:
                block_stats.add_block_event_by_block_hash(
                    block_hash,
                    BlockStatEventType.REMOTE_BLOCK_REQUESTED_BY_GATEWAY,
                    network_num=self.network_num,
                    more_info=f"Protocol: {self.opts.blockchain_protocol}, "
                              f"Network: {self.opts.blockchain_network}"
                )
            self._requested_remote_blocks_queue.append(block_hashes)

    def log_received_remote_blocks(self, blocks_count: int) -> None:
        if len(self._requested_remote_blocks_queue) > 0:
            expected_blocks = self._requested_remote_blocks_queue.pop()

            if len(expected_blocks) != blocks_count:
                logger.warning(log_messages.BLOCK_COUNT_MISMATCH,
                    blocks_count, len(expected_blocks))
                self._skip_remote_block_requests_stats_count = len(self._requested_remote_blocks_queue) * 2
                self._requested_remote_blocks_queue.clear()
                return

            for block_hash in expected_blocks:
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.REMOTE_BLOCK_RECEIVED_BY_GATEWAY,
                                                          network_num=self.network_num,
                                                          more_info="Protocol: {}, Network: {}".format(
                                                              self.opts.blockchain_protocol,
                                                              self.opts.blockchain_network))
        else:
            logger.warning(log_messages.UNEXPECTED_BLOCKS)

    def log_closed_connection(self, connection: AbstractConnection):
        if isinstance(connection, EthNodeConnection):
            # pyre-fixme[22]: The cast is redundant.
            eth_node_connection = cast(EthNodeConnection, connection)
            connection_status = connection.connection_protocol.connection_status

            if ConnectionState.INITIALIZED not in eth_node_connection.state:
                logger.info("Failed to connect to Ethereum node. Verify that provided ip address ({}) and port ({}) "
                            "are correct. Verify that firewall port is open. Connection details: {}.",
                            eth_node_connection.peer_ip, eth_node_connection.peer_port, eth_node_connection)
            elif connection_status.disconnect_message_received:
                logger.info("Connection to Ethereum node failed. Disconnect reason: '{}'. {} Connection details: {}.",
                            # pyre-fixme[6]: Expected `int` for 1st param but got
                            #  `Optional[int]`.
                            self._get_disconnect_reason_description(connection_status.disconnect_reason),
                            # pyre-fixme[6]: Expected `int` for 1st param but got
                            #  `Optional[int]`.
                            self._get_disconnect_reason_instruction(connection_status.disconnect_reason),
                            connection)
            elif not connection_status.auth_message_received and not connection_status.auth_ack_message_received:
                logger.info("Failed to connect to Ethereum node. Verify that '--node-public-key' argument is provided "
                            "and value matches enode of the Ethereum node. Connection details: {}.",
                            eth_node_connection)
            elif connection_status.hello_message_received and connection_status.status_message_sent and \
                not connection_status.status_message_received:
                logger.info("Failed to connect to Ethereum node. Verify that '--blockchain-network' and other "
                            "blockchain network arguments are correct. Connection details: {}.",
                            eth_node_connection)
            else:
                super(EthGatewayNode, self).log_closed_connection(connection)
        elif isinstance(connection, GatewayConnection):
            if ConnectionState.ESTABLISHED not in connection.state:
                logger.debug("Failed to connect to: {}.", connection)
            else:
                logger.debug("Closed connection: {}", connection)
        else:
            super(EthGatewayNode, self).log_closed_connection(connection)

    def init_eth_gateway_stat_logging(self):
        eth_gateway_stats_service.set_node(self)
        self.alarm_queue.register_alarm(eth_gateway_stats_service.interval, eth_gateway_stats_service.flush_info)

    def get_enode(self):
        return \
            f"enode://{convert.bytes_to_hex(self.get_public_key())}@{self.opts.external_ip}:{self.opts.non_ssl_port}"

    def _is_in_local_discovery(self):
        return not self.opts.no_discovery and self._node_public_key is None

    def _is_in_remote_discovery(self):
        return not self.opts.no_discovery and self._remote_public_key is None

    def _get_disconnect_reason_description(self, reason: int):
        if reason not in self.DISCONNECT_REASON_TO_DESCRIPTION:
            return f"Disconnect reason ({reason}) is unknown."

        return self.DISCONNECT_REASON_TO_DESCRIPTION[reason]

    def _get_disconnect_reason_instruction(self, reason: int):
        if reason not in self.DISCONNECT_REASON_TO_INSTRUCTION:
            return ""

        return self.DISCONNECT_REASON_TO_INSTRUCTION[reason]
