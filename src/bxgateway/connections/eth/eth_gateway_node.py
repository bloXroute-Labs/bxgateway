import asyncio
from collections import deque
from typing import Optional, List, cast, Iterator, Union

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.feed.feed import FeedKey
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.network.peer_info import ConnectionPeerInfo
from bxcommon.network.transport_layer_protocol import TransportLayerProtocol
from bxcommon.rpc import rpc_constants
from bxcommon.feed.feed_source import FeedSource
from bxcommon.utils import convert
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.blockchain_utils.eth import crypto_utils, eth_common_constants, eth_common_utils
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import log_messages, gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.eth.eth_base_connection_protocol import EthConnectionProtocolStatus
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_node_discovery_connection import EthNodeDiscoveryConnection
from bxgateway.connections.eth.eth_relay_connection import EthRelayConnection
from bxgateway.connections.eth.eth_remote_connection import EthRemoteConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.feed.eth.eth_new_block_feed import EthNewBlockFeed
from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxgateway.feed.eth.eth_on_block_feed import EthOnBlockFeed, EventNotification
from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxgateway.feed.eth.eth_raw_block import EthRawBlock
from bxgateway.feed.eth.eth_transaction_receipts_feed import EthTransactionReceiptsFeed
from bxgateway.messages.eth import eth_message_converter_factory as converter_factory
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxgateway.rpc.external.eth_ws_proxy_publisher import EthWsProxyPublisher
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.eth.eth_block_processing_service import EthBlockProcessingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.services.eth.eth_normal_block_cleanup_service import EthNormalBlockCleanupService
from bxgateway.testing.eth_lossy_relay_connection import EthLossyRelayConnection
from bxgateway.testing.test_modes import TestModes
from bxgateway.utils.interval_minimum import IntervalMinimum
from bxgateway.utils.running_average import RunningAverage
from bxgateway.utils.stats.eth.eth_gateway_stats_service import eth_gateway_stats_service
from bxgateway.utils.stats.eth_on_block_feed_stats_service import eth_on_block_feed_stats_service
from bxutils import logging
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

    def __init__(self, opts, node_ssl_service: NodeSSLService) -> None:
        super(EthGatewayNode, self).__init__(opts, node_ssl_service,
                                             eth_common_constants.TRACKED_BLOCK_CLEANUP_INTERVAL_S)

        self.remote_blockchain_protocol_version = eth_common_constants.ETH_PROTOCOL_VERSION
        self.remote_blockchain_connection_established = False

        self._node_public_key = None
        self._remote_public_key = None

        if opts.node_public_key is not None:
            self._node_public_key = convert.hex_to_bytes(opts.node_public_key)
        elif opts.blockchain_peers is None:
            raise RuntimeError(
                "128 digit public key must be included with command-line specified blockchain peer.")
        if opts.remote_blockchain_peer is not None:
            if opts.remote_public_key is None:
                raise RuntimeError(
                    "128 digit public key must be included with command-line specified remote blockchain peer.")
            else:
                self._remote_public_key = convert.hex_to_bytes(opts.remote_public_key)

        self.block_processing_service: EthBlockProcessingService = EthBlockProcessingService(self)
        self.block_parts_storage: ExpiringDict[Sha256Hash, NewBlockParts] = ExpiringDict(
            self.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "eth_block_queue_parts",
        )

        # List of know total difficulties, tuples of values (block hash, total difficulty)
        self._last_known_difficulties = deque(maxlen=eth_common_constants.LAST_KNOWN_TOTAL_DIFFICULTIES_MAX_COUNT)

        # queue of the block hashes requested from remote blockchain node during sync
        self._requested_remote_blocks_queue = deque()

        # number of remote block requests to skip in case if requests and responses got out of sync
        self._skip_remote_block_requests_stats_count = 0

        self.init_eth_gateway_stat_logging()
        self.init_eth_on_block_feed_stat_logging()

        self.message_converter = converter_factory.create_eth_message_converter(self.opts)
        self.eth_ws_proxy_publisher = EthWsProxyPublisher(opts.eth_ws_uri, self.feed_manager, self._tx_service, self)
        if self.opts.ws and not self.opts.eth_ws_uri:
            logger.warning(log_messages.ETH_WS_SUBSCRIBER_NOT_STARTED)

        self.average_block_gas_price = RunningAverage(gateway_constants.ETH_GAS_RUNNING_AVERAGE_SIZE)
        self.min_tx_from_node_gas_price = IntervalMinimum(gateway_constants.ETH_MIN_GAS_INTERVAL_S, self.alarm_queue)

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

    def build_block_queuing_service(
        self,
        connection: AbstractGatewayBlockchainConnection
    ) -> EthBlockQueuingService:
        return EthBlockQueuingService(self, connection)

    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        if self.opts.use_extensions:
            from bxgateway.services.eth.eth_extension_block_cleanup_service import EthExtensionBlockCleanupService
            return EthExtensionBlockCleanupService(self, self.network_num)
        else:
            return EthNormalBlockCleanupService(self, self.network_num)

    def on_updated_remote_blockchain_peer(self, outbound_peer) -> int:
        if "node_public_key" not in outbound_peer.attributes:
            logger.warning(log_messages.BLOCKCHAIN_PEER_LACKS_PUBLIC_KEY)
            return constants.SDN_CONTACT_RETRY_SECONDS
        else:
            super(EthGatewayNode, self).on_updated_remote_blockchain_peer(outbound_peer)
            self._remote_public_key = convert.hex_to_bytes(outbound_peer.attributes["node_public_key"])
            return constants.CANCEL_ALARMS

    def get_outbound_peer_info(self) -> List[ConnectionPeerInfo]:
        peers = []

        for peer in self.opts.outbound_peers:
            peers.append(ConnectionPeerInfo(
                IpEndpoint(peer.ip, peer.port), convert.peer_node_to_connection_type(self.NODE_TYPE, peer.node_type)
            ))

        local_protocol = TransportLayerProtocol.UDP if self._is_in_local_discovery() else TransportLayerProtocol.TCP
        for blockchain_peer in self.blockchain_peers:
            peers.append(ConnectionPeerInfo(
                IpEndpoint(blockchain_peer.ip, blockchain_peer.port),
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

    def get_private_key(self) -> bytes:
        return convert.hex_to_bytes(self.opts.private_key)

    def get_public_key(self) -> bytes:
        return crypto_utils.private_to_public_key(self.get_private_key())

    def set_node_public_key(self, discovery_connection, node_public_key) -> None:
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

    def set_remote_public_key(self, discovery_connection, remote_public_key) -> None:
        if not isinstance(discovery_connection, EthNodeDiscoveryConnection):
            raise TypeError("Argument discovery_connection is expected to be of type EthNodeDiscoveryConnection, was {}"
                            .format(type(discovery_connection)))

        if not remote_public_key:
            raise ValueError("remote_public_key argument is required")

        self._remote_public_key = remote_public_key

        # close UDP connection
        discovery_connection.mark_for_close(False)

        remote_blockchain_ip = self.remote_blockchain_ip
        remote_blockchain_port = self.remote_blockchain_port

        assert remote_blockchain_ip is not None and remote_blockchain_port is not None
        # establish TCP connection
        self.enqueue_connection(
            remote_blockchain_ip, remote_blockchain_port, ConnectionType.REMOTE_BLOCKCHAIN_NODE
        )

    def get_node_public_key(self, ip: str, port: int) -> bytes:
        node_public_key = None
        for blockchain_peer in self.blockchain_peers:
            if blockchain_peer.ip == ip and blockchain_peer.port == port:
                node_public_key = blockchain_peer.node_public_key
                break

        if not node_public_key:
            raise RuntimeError(
                f"128 digit public key must be included with for blockchain peer ip {ip} and port {port}."
            )

        return convert.hex_to_bytes(node_public_key)

    def get_remote_public_key(self) -> bytes:
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

    def _should_decrease_version_number(
        self, connection_state: ConnectionState, connection_status: EthConnectionProtocolStatus
    ) -> bool:
        return (
            ConnectionState.ESTABLISHED not in connection_state
            and connection_status.auth_message_sent
            and not connection_status.auth_message_received
            and not connection_status.auth_ack_message_sent
            and connection_status.auth_ack_message_received
            and connection_status.hello_message_sent
            and connection_status.hello_message_received
            and not connection_status.status_message_sent
            and not connection_status.status_message_received
            and connection_status.disconnect_message_received
            and connection_status.disconnect_reason == 3
        )

    def _get_new_protocol_version(self, connection, peer_version) -> int:
        for index, version in enumerate(eth_common_constants.SUPPORTED_ETH_PROTOCOL_VERSION):
            if (
                version == peer_version
                and index < len(eth_common_constants.SUPPORTED_ETH_PROTOCOL_VERSION) - 1
            ):
                new_version = eth_common_constants.SUPPORTED_ETH_PROTOCOL_VERSION[index + 1]
                connection.log_debug(
                    "Failed to connect with version {}, try to reconnect with version {}",
                    version, new_version
                )
                return new_version

        return peer_version

    def _should_log_closed_connection(self, connection: AbstractConnection) -> bool:
        if isinstance(connection, EthNodeConnection):
            connection_status = connection.connection_protocol.connection_status

            if (
                self._should_decrease_version_number(connection.state, connection_status)
                and connection.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE
            ):
                for peer in self.blockchain_peers:
                    if (
                        peer == BlockchainPeerInfo(connection.peer_ip, connection.peer_port)
                        and not peer.connection_established
                    ):
                        peer.blockchain_protocol_version = self._get_new_protocol_version(
                            connection, peer.blockchain_protocol_version
                        )
                        return False

        elif isinstance(connection, EthRemoteConnection):
            connection_status = connection.connection_protocol.connection_status

            if (
                self._should_decrease_version_number(connection.state, connection_status)
                and connection.CONNECTION_TYPE == ConnectionType.REMOTE_BLOCKCHAIN_NODE
                and not self.remote_blockchain_connection_established
            ):
                self.remote_blockchain_protocol_version = \
                    self._get_new_protocol_version(
                        connection, self.remote_blockchain_protocol_version
                    )
                return False

        return True

    def log_closed_connection(self, connection: AbstractConnection) -> None:
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
            if connection.established:
                logger.debug("Failed to connect to: {}.", connection)
            else:
                logger.debug("Closed connection: {}", connection)
        else:
            super(EthGatewayNode, self).log_closed_connection(connection)

    def init_eth_gateway_stat_logging(self) -> None:
        eth_gateway_stats_service.set_node(self)
        self.alarm_queue.register_alarm(eth_gateway_stats_service.interval, eth_gateway_stats_service.flush_info)

    def init_eth_on_block_feed_stat_logging(self) -> None:
        eth_on_block_feed_stats_service.set_node(self)
        self.alarm_queue.register_alarm(eth_on_block_feed_stats_service.interval,
                                        eth_on_block_feed_stats_service.flush_info)

    def init_live_feeds(self) -> None:
        self.feed_manager.register_feed(EthNewTransactionFeed())
        self.feed_manager.register_feed(EthPendingTransactionFeed(self.alarm_queue))
        self.feed_manager.register_feed(EthOnBlockFeed(self))
        self.feed_manager.register_feed(EthNewBlockFeed(self))
        self.feed_manager.register_feed(EthTransactionReceiptsFeed(self))

    def on_new_subscriber_request(self) -> None:
        if self.opts.eth_ws_uri and not self.eth_ws_proxy_publisher.running:
            asyncio.create_task(self.eth_ws_proxy_publisher.revive())

    def on_transactions_in_block(self, transactions: List[Transaction]) -> None:
        for transaction in transactions:
            self.average_block_gas_price.add_value(transaction.gas_price)

    def broadcast_transactions_to_nodes(
        self, msg: AbstractMessage, broadcasting_conn: Optional[AbstractConnection]
    ) -> bool:
        msg = cast(TransactionsEthProtocolMessage, msg)

        if self.opts.filter_txs_factor > 0:
            average_block_gas_filter = self.average_block_gas_price.average * self.opts.filter_txs_factor
        else:
            average_block_gas_filter = 0
        min_gas_price_from_node = self.min_tx_from_node_gas_price.current_minimum

        gas_price_filter = max(average_block_gas_filter, min_gas_price_from_node)

        if gas_price_filter > 0:
            assert len(msg.get_transactions()) == 1
            transaction = msg.get_transactions()[0]

            gas_price = float(transaction.gas_price)

            if gas_price < gas_price_filter:
                logger.trace(
                    "Skipping sending transaction {} with gas price: {}. Average was {}. Minimum from node was {}.",
                    transaction.hash(),
                    float(transaction.gas_price),
                    average_block_gas_filter,
                    min_gas_price_from_node
                )
                tx_stats.add_tx_by_hash_event(
                    transaction.hash(),
                    TransactionStatEventType.TX_FROM_BDN_IGNORE_LOW_GAS_PRICE,
                    self.network_num,
                    peers=[broadcasting_conn],
                    more_info="Tx gas price {}. Average block gas price: {}. Node min gas price {}."
                        .format(gas_price, average_block_gas_filter, min_gas_price_from_node)
                )
                return False

        return super().broadcast_transactions_to_nodes(msg, broadcasting_conn)

    def get_enode(self) -> str:
        return \
            f"enode://{convert.bytes_to_hex(self.get_public_key())}@{self.opts.external_ip}:{self.opts.non_ssl_port}"

    def on_block_received_from_bdn(
        self, block_hash: Sha256Hash, block_message: AbstractBlockMessage
    ) -> None:
        super().on_block_received_from_bdn(block_hash, block_message)

        node_conn = cast(EthNodeConnection, self.get_any_active_blockchain_connection())
        if node_conn is not None:
            eth_connection_protocol = node_conn.connection_protocol
            eth_connection_protocol.pending_new_block_parts.remove_item(block_hash)

    # pyre-fixme[14]: Inconsistent override:
    # Parameter of type InternalEthBlockInfo is not a supertype of AbstractBlockMessage
    def publish_block(
        self,
        block_number: Optional[int],
        block_hash: Sha256Hash,
        block_message: Optional[InternalEthBlockInfo],
        source: FeedSource
    ) -> None:
        if block_number is None and block_message is not None:
            block_number = block_message.block_number()
        logger.debug(
            "Handle block notification for feed. Number: {}, Hash: {} Msg: {} From: {}",
            block_number, block_hash, block_message, source
        )
        if block_number and self.opts.ws:
            raw_block = EthRawBlock(
                block_number,
                block_hash,
                source,
                self._get_block_message_lazy(block_message, block_hash)
            )
            self._publish_block_to_on_block_feed(raw_block)
            self._publish_block_to_transaction_receipts_feed(raw_block)
            if block_message is not None:
                self._publish_block_to_new_block_feed(raw_block)

    async def init(self) -> None:
        await super().init()
        try:
            await asyncio.wait_for(
                self.eth_ws_proxy_publisher.start(), rpc_constants.RPC_SERVER_INIT_TIMEOUT_S
            )
        except Exception as e:
            logger.error(log_messages.ETH_WS_INITIALIZATION_FAIL, e)
            self.should_force_exit = True

    async def close(self) -> None:
        try:
            await asyncio.wait_for(
                self.eth_ws_proxy_publisher.stop(),
                rpc_constants.RPC_SERVER_STOP_TIMEOUT_S
            )
        except Exception as e:
            logger.error(log_messages.ETH_WS_CLOSE_FAIL, e, exc_info=True)
        await super().close()

    def _is_in_local_discovery(self) -> bool:
        return not self.opts.no_discovery and self._node_public_key is None

    def _is_in_remote_discovery(self) -> bool:
        return not self.opts.no_discovery and self._remote_public_key is None

    def _get_disconnect_reason_description(self, reason: int) -> str:
        if reason not in self.DISCONNECT_REASON_TO_DESCRIPTION:
            return f"Disconnect reason ({reason}) is unknown."

        return self.DISCONNECT_REASON_TO_DESCRIPTION[reason]

    def _get_disconnect_reason_instruction(self, reason: int) -> str:
        if reason not in self.DISCONNECT_REASON_TO_INSTRUCTION:
            return ""

        return self.DISCONNECT_REASON_TO_INSTRUCTION[reason]

    def _get_block_message_lazy(
        self, block_message: Optional[InternalEthBlockInfo], block_hash
    ) -> Iterator[InternalEthBlockInfo]:
        if block_message:
            yield block_message
        else:
            block_queuing_service = cast(
                EthBlockQueuingService,
                self.block_queuing_service_manager.get_designated_block_queuing_service()
            )
            if block_queuing_service is not None:
                block_parts = block_queuing_service.get_block_parts(block_hash)
                if block_parts:
                    block_message = InternalEthBlockInfo.from_new_block_parts(block_parts)
                    yield block_message

    def _publish_block_to_new_block_feed(
        self,
        raw_block: EthRawBlock,
    ) -> None:
        self.feed_manager.publish_to_feed(FeedKey(EthNewBlockFeed.NAME), raw_block)

    def _publish_block_to_on_block_feed(
        self,
        raw_block: EthRawBlock,
    ) -> None:
        if raw_block.source in EthOnBlockFeed.VALID_SOURCES:
            self.feed_manager.publish_to_feed(
                FeedKey(EthOnBlockFeed.NAME), EventNotification(raw_block.block_number)
            )

    def _publish_block_to_transaction_receipts_feed(
        self,
        raw_block: EthRawBlock
    ):
        self.feed_manager.publish_to_feed(
            FeedKey(EthTransactionReceiptsFeed.NAME), raw_block
        )

    def is_gas_price_above_min_network_fee(self, transaction_contents: Union[memoryview, bytearray]) -> bool:
        gas_price = eth_common_utils.raw_tx_gas_price(memoryview(transaction_contents), 0)
        if gas_price >= self.get_blockchain_network().min_tx_network_fee:
            return True
        return False
