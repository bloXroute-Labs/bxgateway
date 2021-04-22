import asyncio
# TODO: remove try-catch when removing py3.7 support
import functools
import gc
from collections import defaultdict, deque

from bxcommon.utils.stats.memory_statistics_service import memory_statistics
from bxgateway.services.block_queuing_service_manager import BlockQueuingServiceManager
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.feed.new_block_feed import NewBlockFeed
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service

try:
    from asyncio.exceptions import CancelledError
except ImportError:
    from asyncio.futures import CancelledError

import time
from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from typing import Tuple, Optional, ClassVar, Type, Set, List, Iterable, Union, cast, Dict, Deque
from prometheus_client import Gauge

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.models.blockchain_network_model import BlockchainNetworkModel
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.models.node_event_model import NodeEventType
from bxcommon.models.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.network.network_direction import NetworkDirection
from bxcommon.network.peer_info import ConnectionPeerInfo
from bxcommon.services import sdn_http_service
from bxcommon.services.broadcast_service import BroadcastService
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.utils import network_latency, memory_utils, convert, node_cache
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import hooks
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.rpc import rpc_constants
from bxcommon.feed.feed_manager import FeedManager
from bxcommon.feed.feed_source import FeedSource
from bxgateway import gateway_constants
from bxgateway import log_messages
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxcommon.feed.new_transaction_feed import NewTransactionFeed
from bxgateway.gateway_opts import GatewayOpts
from bxgateway.rpc.https.gateway_http_rpc_server import GatewayHttpRpcServer
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.block_recovery_service import BlockRecoveryService, RecoveredTxsSource
from bxgateway.services.gateway_broadcast_service import GatewayBroadcastService
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.utils import configuration_utils
from bxgateway.utils.blockchain_message_queue import BlockchainMessageQueue
from bxgateway.utils.logging.status import status_log
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxgateway.rpc.ws.ws_server import WsServer
from bxgateway.rpc.ipc.ipc_server import IpcServer
from bxutils import logging
from bxutils.services.node_ssl_service import NodeSSLService
from bxutils.ssl.extensions import extensions_factory
from bxutils.ssl.ssl_certificate_type import SSLCertificateType

logger = logging.get_logger(__name__)


class AbstractGatewayNode(AbstractNode, metaclass=ABCMeta):
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.
    """
    NODE_TYPE = NodeType.EXTERNAL_GATEWAY
    # pyre-fixme[8]: Attribute has type `Type[AbstractRelayConnection]`; used as `None`.
    RELAY_CONNECTION_CLS: ClassVar[Type[AbstractRelayConnection]] = None

    remote_blockchain_ip: Optional[str] = None
    remote_blockchain_port: Optional[int] = None
    remote_node_conn: Optional[AbstractGatewayBlockchainConnection] = None
    remote_blockchain_protocol_version: Optional[int] = None
    remote_blockchain_connection_established: bool = False
    transaction_streamer_peer: Optional[OutboundPeerModel] = None

    _blockchain_liveliness_alarm: Optional[AlarmId] = None
    _relay_liveliness_alarm: Optional[AlarmId] = None
    _active_blockchain_peers_alarm: Optional[AlarmId] = None

    remote_node_msg_queue: BlockchainMessageQueue
    msg_proxy_requester_queue: Deque[AbstractGatewayBlockchainConnection]

    peer_gateways: Set[OutboundPeerModel]
    # if opts.split_relays is set, then this set contains only block relays
    peer_relays: Set[OutboundPeerModel]
    peer_transaction_relays: Set[OutboundPeerModel]
    blockchain_peers: Set[BlockchainPeerInfo]
    num_active_blockchain_peers: int
    blocks_seen: ExpiringSet
    in_progress_blocks: BlockEncryptedCache
    block_recovery_service: BlockRecoveryService
    blockchain_peer_to_block_queuing_service: Dict[AbstractGatewayBlockchainConnection, AbstractBlockQueuingService]
    block_storage: ExpiringDict[Sha256Hash, Optional[AbstractBlockMessage]]
    block_queuing_service_manager: BlockQueuingServiceManager
    block_processing_service: BlockProcessingService
    block_cleanup_service: AbstractBlockCleanupService
    _tx_service: GatewayTransactionService

    _block_from_node_handling_times: ExpiringDict[Sha256Hash, float]
    _block_from_bdn_handling_times: ExpiringDict[Sha256Hash, Tuple[float, str]]

    time_blockchain_peer_conn_destroyed_by_ip: Dict[Tuple[str, int], float] = defaultdict(float)

    feed_manager: FeedManager
    has_feed_subscribers: bool

    tracked_block_cleanup_interval_s: float
    account_model: Optional[BdnAccountModelBase]

    def __init__(
        self,
        opts: GatewayOpts,
        node_ssl_service: NodeSSLService,
        tracked_block_cleanup_interval_s=constants.CANCEL_ALARMS
    ) -> None:
        super(AbstractGatewayNode, self).__init__(
            opts, node_ssl_service
        )
        self.opts: GatewayOpts = opts
        if opts.split_relays:
            opts.peer_transaction_relays = {
                OutboundPeerModel(peer_relay.ip, peer_relay.port + 1, node_type=NodeType.RELAY_TRANSACTION)
                for peer_relay in opts.peer_relays
            }
            opts.outbound_peers.update(opts.peer_transaction_relays)
        else:
            opts.peer_transaction_relays = set()

        if opts.account_model:
            self.account_model = opts.account_model
            node_cache.update_cache_file(
                opts,
                # pyre-fixme[16]: `Optional` has no attribute `account_id`
                accounts={opts.account_model.account_id: self.account_model},
            )
        else:
            node_cache_info = node_cache.read(opts)
            if (
                node_cache_info
                and node_cache_info.accounts
                and node_cache_info.node_model
                and node_cache_info.node_model.account_id is not None
            ):
                # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`
                self.account_model = node_cache_info.accounts[node_cache_info.node_model.account_id]
            else:
                self.account_model = None

        self.quota_level = 0
        self.num_active_blockchain_peers = 0
        self.blockchain_peers = self.get_blockchain_peers_from_cache_and_command_line()
        self.transaction_streamer_peer = None
        self.peer_gateways = set(opts.peer_gateways)
        self.peer_relays = set(opts.peer_relays)
        self.peer_transaction_relays = set(opts.peer_transaction_relays)
        self.peer_relays_min_count = 1

        self.remote_node_msg_queue = BlockchainMessageQueue(opts.remote_blockchain_message_ttl)
        self.msg_proxy_requester_queue: Deque[AbstractGatewayBlockchainConnection] = deque(
            maxlen=gateway_constants.MSG_PROXY_REQUESTER_QUEUE_LIMIT
        )

        self.blocks_seen = ExpiringSet(
            self.alarm_queue,
            gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S,
            "gateway_blocks_seen"
        )
        self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)
        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)
        self.neutrality_service = NeutralityService(self)
        self.block_processing_service = BlockProcessingService(self)
        self.block_cleanup_service = self.build_block_cleanup_service()

        self.block_storage = ExpiringDict(
            self.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "block_queuing_service_blocks"
        )
        self.blockchain_peer_to_block_queuing_service = {}
        self.block_queuing_service_manager = BlockQueuingServiceManager(
            self.block_storage,
            self.blockchain_peer_to_block_queuing_service
        )

        self.send_request_for_relay_peers_num_of_calls = 0
        self.check_relay_alarm_id: Optional[AlarmId] = None
        self.transaction_sync_start_alarm_id: Optional[AlarmId] = None

        if not self.opts.peer_relays:
            self._schedule_fetch_relays_from_sdn()
        else:
            self._schedule_fetch_relays_from_sdn(
                gateway_constants.RELAY_CONNECTION_REEVALUATION_INTERVAL_S
            )

        if opts.connect_to_remote_blockchain:
            if opts.remote_blockchain_peer is not None:
                self.remote_blockchain_ip = opts.remote_blockchain_ip
                self.remote_blockchain_port = opts.remote_blockchain_port
                self.enqueue_connection(
                    opts.remote_blockchain_ip, opts.remote_blockchain_port, ConnectionType.REMOTE_BLOCKCHAIN_NODE
                )
            else:
                # offset SDN calls so all the peers aren't queued up at the same time
                self.alarm_queue.register_alarm(
                    constants.SDN_CONTACT_RETRY_SECONDS + 1,
                    self.send_request_for_remote_blockchain_peer
                )

        # offset SDN calls so all the peers aren't queued up at the same time
        self.send_request_for_gateway_peers_num_of_calls = 0
        self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS + 2, self._send_request_for_gateway_peers)
        self.network = self.get_blockchain_network()

        if opts.use_extensions:
            from bxgateway.services.extension_gateway_transaction_service import ExtensionGatewayTransactionService
            self._tx_service = ExtensionGatewayTransactionService(self, self.network_num)
        else:
            self._tx_service = GatewayTransactionService(self, self.network_num)

        self.init_transaction_stat_logging()
        self.init_bdn_performance_stats_logging()
        self.init_node_config_update()
        self.init_transaction_feed_stat_logging()

        self._block_from_node_handling_times = ExpiringDict(
            self.alarm_queue,
            gateway_constants.BLOCK_HANDLING_TIME_EXPIRATION_TIME_S,
            f"gateway_block_from_node_handling_times",
        )
        self._block_from_bdn_handling_times = ExpiringDict(
            self.alarm_queue,
            gateway_constants.BLOCK_HANDLING_TIME_EXPIRATION_TIME_S,
            f"gateway_block_from_bdn_handling_times",
        )

        self.schedule_blockchain_liveliness_check(opts.initial_liveliness_check)
        self.schedule_relay_liveliness_check(opts.initial_liveliness_check)
        self.schedule_active_blockchain_peers_liveliness_check(
            gateway_constants.ACTIVE_BLOCKCHAIN_PEERS_LIVELINESS_CHECK_S
        )

        self.opts.has_fully_updated_tx_service = False

        self.block_cleanup_processed_blocks = ExpiringSet(
            self.alarm_queue,
            gateway_constants.BLOCK_CONFIRMATION_EXPIRE_TIME_S,
            "gateway_block_cleanup_processed_blocks"
        )

        self.message_converter: Optional[AbstractMessageConverter] = None
        self.account_id: Optional[str] = extensions_factory.get_account_id(
            node_ssl_service.get_certificate(SSLCertificateType.PRIVATE)
        )
        self.default_tx_flag: TransactionFlag = opts.default_tx_flag
        if self.default_tx_flag & TransactionFlag.PAID_TX and self.account_id is None:
            logger.error(log_messages.INVALID_ACCOUNT_ID)
            self.default_tx_flag = TransactionFlag.NO_FLAGS

        self.has_feed_subscribers = False
        self.feed_manager = FeedManager(self)
        self._rpc_server = self.build_rpc_server()
        self._ws_server = self.build_ws_server()
        self._ipc_server = IpcServer(opts.ipc_file, self.feed_manager, self)
        self.init_authorized_live_feeds()

        self.tracked_block_cleanup_interval_s = tracked_block_cleanup_interval_s
        if self.tracked_block_cleanup_interval_s > 0:
            self.alarm_queue.register_alarm(self.tracked_block_cleanup_interval_s, self._tracked_block_cleanup,
                                            alarm_name="tracked_blocks_cleanup")

        self.quota_gauge = Gauge(
            "quota_level",
            "Quota being used",
            ("quota",)
        )
        self.quota_gauge.labels("tx").set_function(
            functools.partial(lambda: int(self.quota_level))
        )
        self.additional_servers = Gauge(
            "additional_servers",
            "Status of additional servers",
            ("servers",)
        )
        self.additional_servers.labels("ws_server").set_function(
            functools.partial(self._ws_server.status)
        )
        self.additional_servers.labels("ipc_server").set_function(
            functools.partial(self._ipc_server.status)
        )
        self.additional_servers.labels("rpc_server").set_function(
            functools.partial(self._rpc_server.status)
        )

        status_log.initialize(
            self.opts.use_extensions,
            str(self.opts.source_version),
            self.opts.external_ip,
            self.opts.continent,
            self.opts.country,
            self.opts.should_update_source_version,
            self.account_id,
            self.quota_level
        )

        if self.opts.should_restart_on_high_memory:
            self.alarm_queue.register_alarm(
                gateway_constants.CHECK_MEMORY_THRESHOLD_INTERVAL_S,
                self._check_memory_threshold,
                alarm_name="check_memory_threshold"
            )

    @abstractmethod
    def build_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    @abstractmethod
    def build_relay_connection(self, socket_connection: AbstractSocketConnectionProtocol) -> AbstractRelayConnection:
        pass

    @abstractmethod
    def build_remote_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    def build_gateway_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> GatewayConnection:
        return GatewayConnection(socket_connection, self)

    @abstractmethod
    def build_block_queuing_service(
        self,
        connection: AbstractGatewayBlockchainConnection
    ) -> AbstractBlockQueuingService:
        pass

    @abstractmethod
    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        pass

    def build_rpc_server(self) -> GatewayHttpRpcServer:
        return GatewayHttpRpcServer(self)

    def build_ws_server(self) -> WsServer:
        return WsServer(self.opts.ws_host, self.opts.ws_port, self.feed_manager, self)

    def get_broadcast_service(self) -> BroadcastService:
        return GatewayBroadcastService(self.connection_pool)

    def init_transaction_stat_logging(self) -> None:
        gateway_transaction_stats_service.set_node(self)
        self.alarm_queue.register_alarm(gateway_transaction_stats_service.interval,
                                        gateway_transaction_stats_service.flush_info)

    def init_bdn_performance_stats_logging(self) -> None:
        gateway_bdn_performance_stats_service.set_node(self)
        self.alarm_queue.register_alarm(
            gateway_bdn_performance_stats_service.interval, self.send_bdn_performance_stats
        )

    def init_transaction_feed_stat_logging(self) -> None:
        transaction_feed_stats_service.set_node(self)
        self.alarm_queue.register_alarm(
            transaction_feed_stats_service.interval,
            transaction_feed_stats_service.flush_info
        )

    def init_authorized_live_feeds(self) -> None:
        new_transaction_streaming_valid = False
        account_model = self.account_model
        if account_model:
            new_transaction_streaming = account_model.new_transaction_streaming
            if new_transaction_streaming:
                new_transaction_streaming_valid = new_transaction_streaming.is_service_valid()
        if self.opts.ws and new_transaction_streaming_valid:
            self.init_live_feeds()
        elif self.opts.ws:
            logger.warning(log_messages.TRANSACTION_FEED_NOT_ALLOWED)

    def init_live_feeds(self) -> None:
        self.feed_manager.register_feed(NewTransactionFeed())
        self.feed_manager.register_feed(NewBlockFeed())

    def send_bdn_performance_stats(self) -> int:
        relay_connection = next(iter(self.connection_pool.get_by_connection_types((ConnectionType.RELAY_BLOCK,))), None)
        if not relay_connection:
            gateway_bdn_performance_stats_service.create_interval_data_object()
            return gateway_bdn_performance_stats_service.interval

        gateway_bdn_performance_stats_service.close_interval_data()
        relay_connection.send_bdn_performance_stats(gateway_bdn_performance_stats_service.interval_data)
        gateway_bdn_performance_stats_service.create_interval_data_object()
        return gateway_bdn_performance_stats_service.interval

    def init_node_config_update(self) -> None:
        self.update_node_config()
        self.alarm_queue.register_alarm(constants.ALARM_QUEUE_INIT_EVENT, self.update_node_config)

    def update_node_config(self) -> int:
        configuration_utils.update_node_config(self)
        return self.opts.config_update_interval

    def _record_mem_stats(self, include_data_structure_memory: bool = False):
        super(AbstractGatewayNode, self)._record_mem_stats(include_data_structure_memory)
        self._tx_service.log_tx_service_mem_stats(include_data_structure_memory)
        if include_data_structure_memory:
            block_cleanup_service_size = memory_utils.get_special_size(self.block_cleanup_service).size
            hooks.add_obj_mem_stats(
                self.__class__.__name__,
                0,
                self.block_cleanup_service,
                "block_cleanup_service",
                memory_utils.ObjectSize(
                    size=block_cleanup_service_size,
                    flat_size=0,
                    is_actual_size=True),
                object_type=memory_utils.ObjectType.META,
                size_type=memory_utils.SizeType.SPECIAL
            )
            for block_queuing_service in self.block_queuing_service_manager:
                block_queuing_service.log_memory_stats()

    def get_tx_service(self, network_num=None) -> GatewayTransactionService:
        if network_num is not None and network_num != self.opts.blockchain_network_num:
            raise ValueError("Gateway is running with network number '{}' but tx service for '{}' was requested"
                             .format(self.opts.blockchain_network_num, network_num))

        return self._tx_service

    def on_fully_updated_tx_service(self) -> None:
        super().on_fully_updated_tx_service()
        self.requester.send_threaded_request(sdn_http_service.submit_tx_synced_event,
                                             self.opts.node_id)

    def track_block_from_node_handling_started(self, block_hash: Sha256Hash) -> None:
        """
        Tracks that node started handling a block received from blockchain node
        :param block_hash: block hash
        """
        if block_hash not in self._block_from_node_handling_times.contents:
            self._block_from_node_handling_times.add(block_hash, time.time())

    def track_block_from_node_handling_ended(self, block_hash: Sha256Hash) -> float:
        """
        Tracks that node ended handling of a block received from blockchain node and returns total handling duration
        :param block_hash: block hash
        :return: handling duration in ms
        """

        if block_hash not in self._block_from_node_handling_times.contents:
            return 0

        handling_start_time = self._block_from_node_handling_times.contents[block_hash]
        duration = (time.time() - handling_start_time) * 1000
        self._block_from_node_handling_times.remove_item(block_hash)

        return duration

    def track_block_from_bdn_handling_started(self, block_hash: Sha256Hash, relay_description: str) -> None:
        """
        Tracks that node started handling a block received from BDN
        :param block_hash: block hash
        :param relay_description: description of the relay peer where block was received
        """
        if block_hash not in self._block_from_bdn_handling_times.contents:
            self._block_from_bdn_handling_times.add(block_hash, (time.time(), relay_description))

    def track_block_from_bdn_handling_ended(self, block_hash: Sha256Hash) -> Tuple[float, Optional[str]]:
        """
        Tracks that node ended handling of a block received from BDN and returns total handling duration
        :param block_hash: block hash
        :return: Tuple (handling duration in ms, relay peer description)
        """

        if block_hash not in self._block_from_bdn_handling_times.contents:
            return 0, None

        handling_start_time, relay_description = self._block_from_bdn_handling_times.contents[block_hash]
        duration = (time.time() - handling_start_time) * 1000
        self._block_from_bdn_handling_times.remove_item(block_hash)

        return duration, relay_description

    async def init(self) -> None:
        await super(AbstractGatewayNode, self).init()
        if self.opts.rpc:
            try:
                await asyncio.wait_for(
                    self._rpc_server.start(), rpc_constants.RPC_SERVER_INIT_TIMEOUT_S
                )
            except Exception as e:
                logger.error(log_messages.RPC_INITIALIZATION_FAIL, e, exc_info=True)

        if self.opts.ws:
            try:
                await asyncio.wait_for(
                    self._ws_server.start(), rpc_constants.RPC_SERVER_INIT_TIMEOUT_S
                )
            except Exception as e:
                logger.error(log_messages.WS_INITIALIZATION_FAIL, e, exc_info=True)

        if self.opts.ipc:
            try:
                await asyncio.wait_for(
                    self._ipc_server.start(), rpc_constants.RPC_SERVER_INIT_TIMEOUT_S
                )
            except Exception as e:
                logger.error(log_messages.IPC_INITIALIZATION_FAIL, e, exc_info=True)

    async def close(self) -> None:
        try:
            await asyncio.wait_for(self._rpc_server.stop(), rpc_constants.RPC_SERVER_STOP_TIMEOUT_S)
        except (Exception, CancelledError) as e:
            logger.error(log_messages.RPC_CLOSE_FAIL, e, exc_info=True)
        try:
            await asyncio.wait_for(self._ws_server.stop(), rpc_constants.RPC_SERVER_STOP_TIMEOUT_S)
        except (Exception, CancelledError) as e:
            logger.error(log_messages.WS_CLOSE_FAIL, e, exc_info=True)
        try:
            await asyncio.wait_for(self._ipc_server.stop(), rpc_constants.RPC_SERVER_STOP_TIMEOUT_S)
        except (Exception, CancelledError) as e:
            logger.error(log_messages.IPC_CLOSE_FAIL, e, exc_info=True)

        await super(AbstractGatewayNode, self).close()

    def send_request_for_remote_blockchain_peer(self):
        """
        Requests a bloxroute owned blockchain node from the SDN.
        """
        self.requester.send_threaded_request(
            sdn_http_service.fetch_remote_blockchain_peer,
            self.opts.node_id,
            done_callback=self._process_remote_blockchain_peer_from_sdn
        )

    def get_outbound_peer_info(self) -> List[ConnectionPeerInfo]:
        peers = [
            ConnectionPeerInfo(
                IpEndpoint(peer.ip, peer.port),
                convert.peer_node_to_connection_type(self.NODE_TYPE, peer.node_type)
            )
            for peer in self.outbound_peers
        ]
        for blockchain_peer in self.blockchain_peers:
            peers.append(ConnectionPeerInfo(
                IpEndpoint(blockchain_peer.ip, blockchain_peer.port), ConnectionType.BLOCKCHAIN_NODE
            ))
        if self.remote_blockchain_ip is not None and self.remote_blockchain_port is not None:
            peers.append(ConnectionPeerInfo(
                # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
                IpEndpoint(self.remote_blockchain_ip, self.remote_blockchain_port),
                ConnectionType.REMOTE_BLOCKCHAIN_NODE)
            )
        return peers

    def build_connection(self, socket_connection: AbstractSocketConnectionProtocol) -> Optional[AbstractConnection]:
        """
        Builds a connection class object based on the characteristics of the ip, port, and direction of the connection.

        For split relays, force shadowing override of the connection's `CONNECTION_TYPE` attribute. This isn't ideal,
        but prevents the necessity of writing many types of connection classes that differ only by the `CONNECTION_TYPE`
        attribute and lots of branching conditional logic.

        :param socket_connection: socket connection accepting or initializing connection
        :return: connection object, if connection class is found
        """
        ip, port = socket_connection.endpoint
        from_me = socket_connection.direction == NetworkDirection.OUTBOUND
        self.alarm_queue.register_approx_alarm(2 * constants.MIN_SLEEP_TIMEOUT, constants.MIN_SLEEP_TIMEOUT,
                                               status_log.update_alarm_callback, self.connection_pool,
                                               self.opts.use_extensions, self.opts.source_version,
                                               self.opts.external_ip, self.opts.continent, self.opts.country,
                                               self.opts.should_update_source_version, self.blockchain_peers,
                                               self.account_id, self.quota_level)
        if self.is_blockchain_peer(ip, port):
            return self.build_blockchain_connection(socket_connection)
        elif self.remote_blockchain_ip == ip and self.remote_blockchain_port == port:
            return self.build_remote_blockchain_connection(socket_connection)
        # only other gateways attempt to actively connect to gateways
        elif not from_me or any(ip == peer_gateway.ip and port == peer_gateway.port
                                for peer_gateway in self.peer_gateways):
            return self.build_gateway_connection(socket_connection)
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_relays):
            relay_connection = self.build_relay_connection(socket_connection)
            if self.opts.split_relays:
                relay_connection.CONNECTION_TYPE = ConnectionType.RELAY_BLOCK
                relay_connection.format_connection()
                relay_connection.disable_buffering()
            return relay_connection
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_transaction_relays):
            assert self.opts.split_relays
            relay_connection = self.build_relay_connection(socket_connection)
            relay_connection.CONNECTION_TYPE = ConnectionType.RELAY_TRANSACTION
            relay_connection.format_connection()
            return relay_connection
        else:
            logger.debug("Attempted connection to peer that's not a blockchain, remote blockchain, gateway, or relay. "
                         "Tried: {}:{}, from_me={}. Ignoring.", ip, port, from_me)
            return None

    def is_blockchain_peer(self, ip, port) -> bool:
        return BlockchainPeerInfo(ip, port) in self.blockchain_peers

    def broadcast_transactions_to_nodes(
        self, msg: AbstractMessage, broadcasting_conn: Optional[AbstractConnection]
    ) -> bool:
        self.broadcast(
            msg,
            broadcasting_conn=broadcasting_conn,
            connection_types=(ConnectionType.BLOCKCHAIN_NODE,)
        )
        return True

    def send_msg_to_remote_node(self, msg: AbstractMessage) -> None:
        """
        Sends a message to remote connected blockchain node.
        """
        remote_node_conn = self.remote_node_conn
        if remote_node_conn is not None:
            remote_node_conn.enqueue_msg(msg)
        else:
            logger.trace("Adding message to remote node's message queue: {}", msg)
            self.remote_node_msg_queue.append(msg)

    def continue_retrying_connection(self, ip: str, port: int, connection_type: ConnectionType) -> bool:
        if connection_type == ConnectionType.BLOCKCHAIN_NODE:
            self.num_active_blockchain_peers = len(
                list(
                    self.connection_pool.get_by_connection_types(
                        (ConnectionType.BLOCKCHAIN_NODE,)
                    )
                )
            )
            return self.num_active_blockchain_peers == 0 \
                or ((ip, port) in self.time_blockchain_peer_conn_destroyed_by_ip and
                    time.time() - self.time_blockchain_peer_conn_destroyed_by_ip[(ip, port)]
                    < gateway_constants.MULTIPLE_BLOCKCHAIN_NODE_MAX_RETRY_S)
        else:
            return (super(AbstractGatewayNode, self).continue_retrying_connection(ip, port, connection_type)
                    or OutboundPeerModel(ip, port) in self.opts.peer_gateways
                    or (connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE and
                        self.num_retries_by_ip[(ip, port)] < gateway_constants.REMOTE_BLOCKCHAIN_MAX_CONNECT_RETRIES))

    def on_blockchain_connection_ready(self, connection: AbstractGatewayBlockchainConnection) -> None:
        new_blockchain_peer = BlockchainPeerInfo(connection.peer_ip, connection.peer_port)
        self.blockchain_peers.add(new_blockchain_peer)
        if connection not in self.block_queuing_service_manager.blockchain_peer_to_block_queuing_service:
            block_queuing_service = self.build_block_queuing_service(connection)
            self.block_queuing_service_manager.add_block_queuing_service(connection, block_queuing_service)
        else:
            queuing_service = self.block_queuing_service_manager.blockchain_peer_to_block_queuing_service[connection]
            queuing_service.update_connection(connection)

        self.cancel_blockchain_liveliness_check()
        self.alarm_queue.register_alarm(
            gateway_constants.CHECK_BLOCKCHAIN_CONNECTION_FIRMLY_ESTABLISHED,
            self.check_blockchain_connection_firmly_established,
            new_blockchain_peer
        )
        self.requester.send_threaded_request(
            sdn_http_service.submit_peer_connection_event,
            NodeEventType.BLOCKCHAIN_NODE_CONN_ESTABLISHED,
            self.opts.node_id,
            connection.peer_ip,
            connection.peer_port
        )
        self.num_active_blockchain_peers = len(
            list(
                self.connection_pool.get_by_connection_types(
                    (ConnectionType.BLOCKCHAIN_NODE,)
                )
            )
        )

    def on_blockchain_connection_destroyed(self, connection: AbstractGatewayBlockchainConnection) -> None:
        if BlockchainPeerInfo(connection.peer_ip, connection.peer_port) in self.blockchain_peers:
            event_type = NodeEventType.BLOCKCHAIN_NODE_CONN_ERR
            if (connection.peer_ip, connection.peer_port) not in self.time_blockchain_peer_conn_destroyed_by_ip:
                self.time_blockchain_peer_conn_destroyed_by_ip[(connection.peer_ip, connection.peer_port)] = time.time()
        else:
            event_type = NodeEventType.BLOCKCHAIN_NODE_CONN_REMOVED
            self.time_blockchain_peer_conn_destroyed_by_ip.pop((connection.peer_ip, connection.peer_port), None)

        self.requester.send_threaded_request(
            sdn_http_service.submit_peer_connection_event,
            event_type,
            self.opts.node_id,
            connection.peer_ip,
            connection.peer_port,
            connection.get_connection_state_details()
        )
        self.num_active_blockchain_peers = len(
            list(
                self.connection_pool.get_by_connection_types(
                    (ConnectionType.BLOCKCHAIN_NODE,)
                )
            )
        )

        if self.num_active_blockchain_peers > 0:
            self.block_queuing_service_manager.remove_block_queuing_service(connection)

    def log_refused_connection(self, peer_info: ConnectionPeerInfo, error: str) -> None:
        if peer_info.connection_type == ConnectionType.BLOCKCHAIN_NODE:
            logger.info("Failed to connect to: {}, {}. Verify that provided ip address ({}) and port ({}) "
                        "are correct. Verify that firewall port is open.", peer_info, error,
                        peer_info.endpoint.ip_address, peer_info.endpoint.port)
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_event,
                                                 NodeEventType.BLOCKCHAIN_NODE_CONN_ERR,
                                                 self.opts.node_id,
                                                 peer_info.endpoint.ip_address,
                                                 peer_info.endpoint.port,
                                                 "Connection refused")
        elif peer_info.connection_type == ConnectionType.EXTERNAL_GATEWAY:
            logger.debug("Failed to connect to: {}, {}.", peer_info, error)
        else:
            super(AbstractGatewayNode, self).log_refused_connection(peer_info, error)

    def on_remote_blockchain_connection_ready(self, connection: AbstractGatewayBlockchainConnection) -> None:
        for msg in self.remote_node_msg_queue.pop_items():
            connection.enqueue_msg(msg)
        self.remote_node_conn = connection
        self.requester.send_threaded_request(
            sdn_http_service.submit_peer_connection_event,
            NodeEventType.REMOTE_BLOCKCHAIN_CONN_ESTABLISHED,
            self.opts.node_id,
            connection.peer_ip,
            connection.peer_port
        )

    def on_remote_blockchain_connection_destroyed(self, connection: AbstractGatewayBlockchainConnection) -> None:
        self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_event,
                                             NodeEventType.REMOTE_BLOCKCHAIN_CONN_ERR,
                                             self.opts.node_id,
                                             connection.peer_ip,
                                             connection.peer_port)
        self.remote_node_conn = None
        self.remote_node_msg_queue.pop_items()

    def on_relay_connection_ready(self, conn_type: ConnectionType) -> None:
        # when gateway connects to a relay, check if the gateway doesn't have another syncing process and
        # that the connection is relay tx (to avoid double syncing when gateway connects to both block and tx)
        if self.opts.sync_tx_service \
            and self.network_num not in self.last_sync_message_received_by_network \
                and ConnectionType.RELAY_TRANSACTION in conn_type \
                and len(list(self.connection_pool.get_by_connection_types((ConnectionType.RELAY_TRANSACTION,)))) == 1:
            # set sync to false and updating sdn
            self.opts.has_fully_updated_tx_service = False
            self.requester.send_threaded_request(sdn_http_service.submit_tx_not_synced_event, self.opts.node_id)

            alarm_id = self.transaction_sync_start_alarm_id
            if alarm_id:
                self.alarm_queue.unregister_alarm(alarm_id)
            self.transaction_sync_start_alarm_id = \
                self.alarm_queue.register_alarm(constants.FIRST_TX_SERVICE_SYNC_PROGRESS_S, self.sync_tx_services)

        self.cancel_relay_liveliness_check()

    def on_failed_connection_retry(
        self, ip: str, port: int, connection_type: ConnectionType, connection_state: ConnectionState
    ) -> None:
        self.alarm_queue.register_approx_alarm(2 * constants.MIN_SLEEP_TIMEOUT, constants.MIN_SLEEP_TIMEOUT,
                                               status_log.update_alarm_callback, self.connection_pool,
                                               self.opts.use_extensions, self.opts.source_version,
                                               self.opts.external_ip, self.opts.continent, self.opts.country,
                                               self.opts.should_update_source_version, self.blockchain_peers,
                                               self.account_id, self.quota_level)
        if connection_type in ConnectionType.GATEWAY:
            if ConnectionState.ESTABLISHED in connection_state:
                self.requester.send_threaded_request(
                    sdn_http_service.submit_peer_connection_error_event,
                    self.opts.node_id,
                    ip,
                    port
                )
            self._remove_gateway_peer(ip, port)
        elif connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            self.send_request_for_remote_blockchain_peer()
        elif ConnectionType.RELAY_BLOCK in connection_type:
            if ConnectionState.ESTABLISHED in connection_state:
                self.requester.send_threaded_request(
                    sdn_http_service.submit_peer_connection_error_event,
                    self.opts.node_id,
                    ip,
                    port
                )
            self.remove_relay_peer(ip, port)
        elif self.opts.split_relays and ConnectionType.RELAY_TRANSACTION in connection_type:
            if ConnectionState.ESTABLISHED in connection_state:
                self.requester.send_threaded_request(
                    sdn_http_service.submit_peer_connection_error_event,
                    self.opts.node_id,
                    ip,
                    port
                )
            self.remove_relay_transaction_peer(ip, port)

        # Reset number of retries in case if SDN instructs to connect to the same node again
        self.num_retries_by_ip[(ip, port)] = 0

    def on_updated_remote_blockchain_peer(self, outbound_peer) -> int:
        self.remote_blockchain_ip = outbound_peer.ip
        self.remote_blockchain_port = outbound_peer.port
        self.enqueue_connection(outbound_peer.ip, outbound_peer.port, ConnectionType.REMOTE_BLOCKCHAIN_NODE)
        return constants.CANCEL_ALARMS

    def on_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        connection: Optional[AbstractGatewayBlockchainConnection] = None,
        block_message: Optional[AbstractBlockMessage] = None,
        block_number: Optional[int] = None
    ) -> bool:
        if connection:
            block_queuing_service = self.block_queuing_service_manager.get_block_queuing_service(connection)
            if block_queuing_service is not None:
                block_queuing_service.mark_block_seen_by_blockchain_node(
                    block_hash,
                    block_message,
                    block_number
                )

        recovery_canceled = False
        if block_message is not None:
            self.blocks_seen.add(block_hash)
            recovery_canceled = self.block_recovery_service.cancel_recovery_for_block(block_hash)

        if recovery_canceled:
            assert block_message is not None
            self.block_queuing_service_manager.update_recovered_block(block_hash, block_message, connection)
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_CANCELED,
                network_num=self.network_num,
                more_info=RecoveredTxsSource.BLOCK_RECEIVED_FROM_NODE
            )
            logger.debug(
                "Recovery status for block {}: "
                "Block recovery was cancelled by gateway. Reason - {}.",
                block_hash, RecoveredTxsSource.BLOCK_RECEIVED_FROM_NODE
            )

        self.publish_block(
            block_number, block_hash, block_message, FeedSource.BLOCKCHAIN_SOCKET
        )

        self.log_blocks_network_content(self.network_num, block_message)
        return recovery_canceled

    def on_block_received_from_bdn(
        self, block_hash: Sha256Hash, block_message: AbstractBlockMessage
    ) -> None:
        self.blocks_seen.add(block_hash)
        if self.block_queuing_service_manager.get_block_data(block_hash) is None:
            self.block_queuing_service_manager.store_block_data(block_hash, block_message)
        self.block_recovery_service.cancel_recovery_for_block(block_hash)

    def publish_block(
        self,
        block_number: Optional[int],
        block_hash: Sha256Hash,
        block_message: Optional[AbstractBlockMessage],
        source: FeedSource
    ):
        pass

    def log_blocks_network_content(self, network_num: int, block_msg) -> None:
        pass

    def log_txs_network_content(
        self, network_num: int, transaction_hash: Sha256Hash, transaction_contents: Union[bytearray, memoryview]
    ) -> None:
        pass

    def post_block_cleanup_tasks(
        self,
        block_hash: Sha256Hash,
        short_ids: Iterable[int],
        unknown_tx_hashes: Iterable[Sha256Hash]
    ):
        """post cleanup tasks for blocks, override method to implement"""
        pass

    def schedule_blockchain_liveliness_check(self, time_from_now_s: int) -> None:
        if not self._blockchain_liveliness_alarm and self.opts.require_blockchain_connection:
            self._blockchain_liveliness_alarm = self.alarm_queue.register_alarm(time_from_now_s,
                                                                                self.check_blockchain_liveliness)

    def schedule_active_blockchain_peers_liveliness_check(self, time_from_now_s: int) -> None:
        if not self._active_blockchain_peers_alarm and self.opts.require_blockchain_connection:
            self._active_blockchain_peers_alarm = self.alarm_queue.register_alarm(time_from_now_s,
                                                                                  self.check_active_blockchain_peers)

    def schedule_relay_liveliness_check(self, time_from_now_s: int) -> None:
        if not self._relay_liveliness_alarm:
            self._relay_liveliness_alarm = self.alarm_queue.register_alarm(time_from_now_s,
                                                                           self.check_relay_liveliness)

    def cancel_blockchain_liveliness_check(self) -> None:
        blockchain_liveliness_alarm = self._blockchain_liveliness_alarm
        if blockchain_liveliness_alarm:
            self.alarm_queue.unregister_alarm(blockchain_liveliness_alarm)
            self._blockchain_liveliness_alarm = None

    def cancel_relay_liveliness_check(self) -> None:
        relay_liveliness_alarm = self._relay_liveliness_alarm
        if relay_liveliness_alarm:
            self.alarm_queue.unregister_alarm(relay_liveliness_alarm)
            self._relay_liveliness_alarm = None

    def get_blockchain_peers_from_cache_and_command_line(self) -> Set[BlockchainPeerInfo]:
        blockchain_peers = self.opts.blockchain_peers
        blockchain_peers.add(
            BlockchainPeerInfo(self.opts.blockchain_ip, self.opts.blockchain_port, self.opts.node_public_key)
        )
        cache_file_info = node_cache.read(self.opts)
        if cache_file_info is not None:
            blockchain_peers_from_cache = cache_file_info.blockchain_peers
            if blockchain_peers_from_cache is not None:
                for blockchain_peer in blockchain_peers_from_cache:
                    blockchain_peers.add(blockchain_peer)
        return blockchain_peers

    def has_active_blockchain_peer(self) -> bool:
        for conn in self.connection_pool.get_by_connection_types((ConnectionType.BLOCKCHAIN_NODE,)):
            if conn.is_active():
                return True
        return False

    def get_any_active_blockchain_connection(self) -> Optional[AbstractConnection]:
        for conn in self.connection_pool.get_by_connection_types((ConnectionType.BLOCKCHAIN_NODE,)):
            if conn.is_active():
                return conn
        return None

    def check_blockchain_liveliness(self) -> None:
        """
        Checks that the gateway has functional connections to the blockchain node.

        Closes the gateway if it does not, as the gateway is not providing any useful functionality.
        """
        if not self.has_active_blockchain_peer():
            self.should_force_exit = True
            logger.error(log_messages.NO_ACTIVE_BLOCKCHAIN_CONNECTION)

    def check_blockchain_connection_firmly_established(self, blockchain_peer: BlockchainPeerInfo) -> None:
        if self.connection_pool.has_connection(blockchain_peer.ip, blockchain_peer.port):
            self.time_blockchain_peer_conn_destroyed_by_ip.pop((blockchain_peer.ip, blockchain_peer.port), None)

    def check_active_blockchain_peers(self) -> int:
        """
        Checks active blockchain node peers and updates node cache file
        """
        active_blockchain_peers = []
        for peer in self.blockchain_peers:
            if self.connection_pool.has_connection(peer.ip, peer.port):
                active_blockchain_peers.append(peer)
        node_cache.update_cache_file(self.opts, blockchain_peers=active_blockchain_peers)
        return gateway_constants.ACTIVE_BLOCKCHAIN_PEERS_LIVELINESS_CHECK_S

    def check_relay_liveliness(self) -> None:
        if not list(self.connection_pool.get_by_connection_types((ConnectionType.RELAY_ALL,))):
            self.should_force_exit = True
            logger.error(log_messages.NO_ACTIVE_BDN_CONNECTIONS)

    def should_process_block_hash(self, block_hash: Optional[Sha256Hash] = None) -> bool:
        result = True
        reason = None

        if not self.opts.has_fully_updated_tx_service:
            result = False
            reason = "Tx sync in progress"

        if not self.has_active_blockchain_peer():
            result = False
            reason = "No blockchain connection"

        if not result:
            logger.debug(
                "Gateway skipped processing block {}. Reason: {}.",
                block_hash if block_hash is not None else "block message",
                reason
            )
            if block_hash is not None:
                block_stats.add_block_event_by_block_hash(
                    block_hash,
                    BlockStatEventType.ENC_BLOCK_GATEWAY_IGNORE_NO_BLOCKCHAIN,
                    network_num=self.network_num,
                    more_info=reason
                )

        return result

    def reevaluate_transaction_streamer_connection(self) -> None:
        if self.NODE_TYPE is not NodeType.EXTERNAL_GATEWAY:
            return

        if self.transaction_streamer_peer is None:
            logger.error(log_messages.MISSING_TRANSACTION_STREAMER_PEER_INFO)
            return
        transaction_streamer_peer = self.transaction_streamer_peer
        assert transaction_streamer_peer is not None

        now_has_feed_subscribers = self.feed_manager.any_subscribers()
        has_streamer_conn = self.connection_pool.has_connection(
            transaction_streamer_peer.ip,
            transaction_streamer_peer.port,
            transaction_streamer_peer.node_id
        )
        if not self.has_feed_subscribers and now_has_feed_subscribers and not has_streamer_conn:
            self.peer_gateways.add(transaction_streamer_peer)
            self.enqueue_connection(
                transaction_streamer_peer.ip, transaction_streamer_peer.port, convert.peer_node_to_connection_type(
                    self.NODE_TYPE, transaction_streamer_peer.node_type
                )
            )
            self.outbound_peers.add(transaction_streamer_peer)
        elif self.has_feed_subscribers and not now_has_feed_subscribers and has_streamer_conn:
            rem_conn = self.connection_pool.get_by_ipport(
                transaction_streamer_peer.ip, transaction_streamer_peer.port, transaction_streamer_peer.node_id
            )
            if rem_conn:
                rem_conn.mark_for_close(False)
        self.has_feed_subscribers = now_has_feed_subscribers

    def get_network_min_transaction_fee(self) -> int:
        return self.get_blockchain_network().min_tx_network_fee

    def get_blockchain_network(self) -> BlockchainNetworkModel:
        if self.network_num in self.opts.blockchain_networks:
            return self.opts.blockchain_networks[self.network_num]
        raise EnvironmentError(
            f"Unexpectedly did not find network num {self.network_num} in set of blockchain networks: "
            f"{self.opts.blockchain_networks.values()}"
        )

    def sync_tx_services(self) -> int:
        logger.info("Starting to sync transaction state with BDN.")
        super(AbstractGatewayNode, self).sync_tx_services()
        if self.opts.sync_tx_service:
            retry = True
            if self.has_active_blockchain_peer():
                if self.opts.split_relays:
                    relay_tx_connection: Optional[AbstractRelayConnection] = next(
                        iter(self.connection_pool.get_by_connection_types((ConnectionType.RELAY_TRANSACTION,))), None
                    )
                    relay_block_connection: Optional[AbstractRelayConnection] = next(
                        iter(self.connection_pool.get_by_connection_types((ConnectionType.RELAY_BLOCK,))), None
                    )

                    if (
                        relay_tx_connection and relay_block_connection and
                        relay_tx_connection.is_active() and relay_block_connection.is_active()
                    ):
                        if self.transaction_sync_timeout_alarm_id:
                            alarm_id = self.transaction_sync_timeout_alarm_id
                            assert alarm_id is not None
                            self.alarm_queue.unregister_alarm(alarm_id)
                            self.transaction_sync_timeout_alarm_id = None
                        self.transaction_sync_timeout_alarm_id = self.alarm_queue.register_alarm(
                            constants.TX_SERVICE_CHECK_NETWORKS_SYNCED_S, self._transaction_sync_timeout
                        )

                        # the sync with relay_tx must be the last one. since each call erase the previous call alarm
                        relay_block_connection.tx_sync_service.send_tx_service_sync_req(self.network_num)
                        relay_tx_connection.tx_sync_service.send_tx_service_sync_req(self.network_num)
                        self._clear_transaction_service()
                        retry = False
                else:
                    relay_connection: Optional[AbstractRelayConnection] = next(
                        iter(self.connection_pool.get_by_connection_types((ConnectionType.RELAY_ALL,))), None
                    )
                    if relay_connection and relay_connection.is_active():
                        if self.transaction_sync_timeout_alarm_id:
                            alarm_id = self.transaction_sync_timeout_alarm_id
                            assert alarm_id is not None
                            self.alarm_queue.unregister_alarm(alarm_id)
                            self.transaction_sync_timeout_alarm_id = None

                        self.transaction_sync_timeout_alarm_id = self.alarm_queue.register_alarm(
                            constants.TX_SERVICE_CHECK_NETWORKS_SYNCED_S, self._transaction_sync_timeout
                        )
                        relay_connection.tx_sync_service.send_tx_service_sync_req(self.network_num)
                        self._clear_transaction_service()
                        retry = False

            if retry:
                logger.info("Relay connection is not ready to sync transaction state with BDN. Scheduling retry.")
                return constants.TX_SERVICE_SYNC_PROGRESS_S
        else:
            self.on_fully_updated_tx_service()
        return constants.CANCEL_ALARMS

    def check_sync_relay_connections(self, conn: AbstractConnection) -> int:
        if (
            self.network_num in self.last_sync_message_received_by_network
            and time.time() - self.last_sync_message_received_by_network[self.network_num]
            > constants.LAST_MSG_FROM_RELAY_THRESHOLD_S
        ):
            logger.warning(
                log_messages.RELAY_CONNECTION_TIMEOUT, constants.LAST_MSG_FROM_RELAY_THRESHOLD_S
            )

            # restart the sync process
            conn: InternalNodeConnection = cast(InternalNodeConnection, conn)
            conn.tx_sync_service.send_tx_service_sync_req(self.network_num)

        return constants.LAST_MSG_FROM_RELAY_THRESHOLD_S

    def sync_and_send_request_for_relay_peers(self, network_num: int) -> int:
        return super().sync_and_send_request_for_relay_peers(self.network_num)

    # pyre-fixme[14]: `process_potential_relays_from_sdn` overrides method defined in `AbstractNode` inconsistently.
    def process_potential_relays_from_sdn(self, get_potential_relays_future: Future):
        """
        part of sync_and_send_request_for_relay_peers logic
        This function is called once there is a result from SDN regarding potential_relays.
        The process of potential_relays should be run in the main thread, hence the register_alarm
        :param get_potential_relays_future: the list of potentital relays from the BDN API
        :return:
        """

        try:
            potential_relay_peers = get_potential_relays_future.result()

            if potential_relay_peers:
                node_cache.update_cache_file(self.opts, potential_relay_peers=potential_relay_peers)
            else:
                # if called too many times to sync_and_send_request_for_relay_peers,
                # reset relay peers to empty list in cache file
                if (
                    self.send_request_for_relay_peers_num_of_calls
                    > gateway_constants.SEND_REQUEST_RELAY_PEERS_MAX_NUM_OF_CALLS
                ):
                    node_cache.update_cache_file(self.opts, potential_relay_peers=[])
                    self.send_request_for_relay_peers_num_of_calls = 0
                self.send_request_for_relay_peers_num_of_calls += 1

                cache_file_info = node_cache.read(self.opts)
                if cache_file_info is not None:
                    potential_relay_peers = cache_file_info.relay_peers

                if not potential_relay_peers:
                    self._schedule_fetch_relays_from_sdn()
                    return

            # check the network latency using the thread pool
            self.requester.send_threaded_request(
                self._find_best_relay_peers,
                potential_relay_peers,
                done_callback=self.register_potential_relay_peers_from_future
            )
        except Exception as e:
            logger.info("Got {} when trying to read potential_relays from sdn", e)

    def register_potential_relay_peers_from_future(self, ping_potential_relays_future: Future) -> None:
        best_relay_peers = ping_potential_relays_future.result()
        self._register_potential_relay_peers(best_relay_peers)

    def update_node_settings_from_blockchain_network(self, blockchain_network: BlockchainNetworkModel) -> None:
        self.opts.enable_block_compression = blockchain_network.enable_block_compression

    def get_gateway_transaction_streamers_count(self) -> int:
        return 0

    def is_gas_price_above_min_network_fee(self, transaction_contents: Union[bytearray, memoryview]) -> bool:
        return True

    def on_new_subscriber_request(self) -> None:
        pass

    def init_memory_stats_logging(self):
        memory_statistics.set_node(self)
        memory_statistics.start_recording(
            functools.partial(
                self.record_mem_stats,
                constants.GC_LOW_MEMORY_THRESHOLD,
                constants.GC_MEDIUM_MEMORY_THRESHOLD,
                constants.GC_HIGH_MEMORY_THRESHOLD
            )
        )

    def get_ws_server_status(self) -> bool:
        return self._ws_server.status()

    def _set_transaction_streamer_peer(self) -> None:
        if self.transaction_streamer_peer is not None or self.NODE_TYPE is not NodeType.EXTERNAL_GATEWAY:
            return
        for peer in self.peer_gateways:
            if peer.is_transaction_streamer():
                self.transaction_streamer_peer = peer
                return

    def _process_gateway_peers_from_sdn(self, get_gateway_peers_future: Future) -> None:
        try:
            gateway_peers = get_gateway_peers_future.result()
            if (
                not gateway_peers
                and not self.peer_gateways
                and self.send_request_for_gateway_peers_num_of_calls
                < gateway_constants.SEND_REQUEST_GATEWAY_PEERS_MAX_NUM_OF_CALLS
            ):
                # Try again later
                logger.warning(log_messages.NO_GATEWAY_PEERS)
                self.send_request_for_gateway_peers_num_of_calls += 1
                if (
                    self.send_request_for_gateway_peers_num_of_calls ==
                    gateway_constants.SEND_REQUEST_GATEWAY_PEERS_MAX_NUM_OF_CALLS
                ):
                    logger.warning(log_messages.ABANDON_GATEWAY_PEER_REQUEST)

                self.alarm_queue.register_alarm(
                    constants.SDN_CONTACT_RETRY_SECONDS, self._send_request_for_gateway_peers
                )
            elif gateway_peers:
                logger.debug("Processing updated peer gateways: {}", gateway_peers)
                self._add_gateway_peers(gateway_peers)
                self._set_transaction_streamer_peer()
                self.on_updated_peers(self._get_all_peers())
                self.send_request_for_gateway_peers_num_of_calls = 0
        except Exception as e:
            logger.info("Got {} when trying to read gateway peers from sdn", e)

    def _send_request_for_gateway_peers(self) -> int:
        """
        Requests gateway peers from SDN. Merges list with provided command line gateways.
        """
        self.requester.send_threaded_request(
            sdn_http_service.fetch_gateway_peers,
            self.opts.node_id,
            self.opts.request_remote_transaction_streaming,
            done_callback=self._process_gateway_peers_from_sdn
        )

        return constants.CANCEL_ALARMS

    def _get_all_peers(self) -> Set[OutboundPeerModel]:
        peers = self.peer_gateways.union(self.peer_relays).union(self.peer_transaction_relays)
        transaction_streamer_peer = self.transaction_streamer_peer
        if transaction_streamer_peer is not None and not self.has_feed_subscribers:
            peers.discard(transaction_streamer_peer)
        return peers

    def _add_gateway_peers(self, gateways_peers: List[OutboundPeerModel]) -> None:
        for gateway_peer in gateways_peers:
            if (
                gateway_peer.ip != self.opts.external_ip
                or gateway_peer.port != self.opts.external_port
            ):
                self.peer_gateways.add(gateway_peer)

    def _remove_gateway_peer(self, ip, port) -> None:
        gateway_to_remove = None
        for peer_gateway in self.peer_gateways:
            if ip == peer_gateway.ip and port == peer_gateway.port:
                gateway_to_remove = peer_gateway
                break

        if gateway_to_remove is not None:
            self.peer_gateways.remove(gateway_to_remove)
            self.outbound_peers = self._get_all_peers()
            if len(self.peer_gateways) < self.opts.min_peer_gateways:
                self.alarm_queue.register_alarm(
                    constants.SDN_CONTACT_RETRY_SECONDS, self._send_request_for_gateway_peers
                )

    def remove_relay_peer(self, ip: str, port: int) -> None:
        """
        Clean up relay peer on connection failure. (after giving up retry)
        Destroys matching transaction relay if split relays enabled.

        :param ip: relay peer ip
        :param port: relay peer port
        """
        self.peer_relays = {peer_relay for peer_relay in self.peer_relays
                            if not (peer_relay.ip == ip and peer_relay.port == port)}
        if self.opts.split_relays:
            if self.connection_pool.has_connection(ip, port + 1):
                transaction_connection = self.connection_pool.get_by_ipport(ip, port + 1)
                logger.debug("Removing relay transaction connection matching block relay host: {}",
                             transaction_connection)
                transaction_connection.mark_for_close(False)
            self.remove_relay_transaction_peer(ip, port + 1, False)

        self.outbound_peers = self._get_all_peers()

        if (
            len(self.peer_relays) < self.peer_relays_min_count
            or len(self.peer_transaction_relays) < self.peer_relays_min_count
        ):
            logger.debug(
                "Removed relay peer with ip {} and port {}. Current number of relay "
                "peers is lower than required. Scheduling request of new relays from BDN.",
                ip,
                port
            )
            self._schedule_fetch_relays_from_sdn()

        # Reset confirmed blocks if gateway lost connection to block relay
        if len(self.peer_relays) == 0:
            self.block_processing_service.reset_last_confirmed_block_parameters()

    def remove_relay_transaction_peer(self, ip: str, port: int, remove_block_relay: bool = True) -> None:
        """
        Clean up relay transactions peer on connection failure. (after giving up retry)
        Destroys matching block relay.
        :param ip: transaction relay peer ip
        :param port: transaction relay peer port
        :param remove_block_relay: if to remove the corresponding block relay (to avoid infinite recursing)
        """
        self.peer_transaction_relays = {peer_relay for peer_relay in self.peer_transaction_relays
                                        if not (peer_relay.ip == ip and peer_relay.port == port)}
        if remove_block_relay and self.connection_pool.has_connection(ip, port - 1):
            block_connection = self.connection_pool.get_by_ipport(ip, port - 1)
            logger.debug("Removing relay block connection matching transaction relay host: {}",
                         block_connection)
            block_connection.mark_for_close(False)
            self.remove_relay_peer(ip, port - 1)

    def _find_active_connection(self, outbound_peers):
        for peer in outbound_peers:
            if self.connection_pool.has_connection(peer.ip, peer.port):
                connection = self.connection_pool.get_by_ipport(peer.ip, peer.port)
                if connection.is_active():
                    return connection
        return None

    def _get_next_retry_timeout(self, ip: str, port: int) -> int:
        base_timeout = super(AbstractGatewayNode, self)._get_next_retry_timeout(ip, port)

        if self.is_blockchain_peer(ip, port):
            return base_timeout + gateway_constants.ADDITIONAL_BLOCKCHAIN_RECONNECT_TIMEOUT_S

        return base_timeout

    def _transaction_sync_timeout(self) -> int:
        logger.warning(log_messages.TX_SYNC_TIMEOUT)
        if self.check_sync_relay_connections_alarm_id:
            alarm_id = self.check_sync_relay_connections_alarm_id
            assert alarm_id is not None
            self.alarm_queue.unregister_alarm(alarm_id)
            self.check_sync_relay_connections_alarm_id = None
        self.transaction_sync_timeout_alarm_id = None
        self.on_network_synced(self.network_num)
        self.on_fully_updated_tx_service()
        return constants.CANCEL_ALARMS

    def _find_best_relay_peers(
        self, potential_relay_peers: List[OutboundPeerModel]
    ) -> List[OutboundPeerModel]:
        logger.info("Received list of potential relays from BDN: {}.",
                    ", ".join([node.ip for node in potential_relay_peers]))

        logger.trace("Potential relay peers: {}", [node.node_id for node in potential_relay_peers])

        best_relay_peers = network_latency.get_best_relays_by_ping_latency_one_per_country(
            potential_relay_peers, gateway_constants.MAX_PEER_RELAYS_COUNT, self.peer_relays
        )

        if not best_relay_peers:
            best_relay_peers = potential_relay_peers

        best_relay_country = best_relay_peers[0].get_country()

        if self.opts.min_peer_relays_count is not None and self.opts.min_peer_relays_count > 0:
            self.peer_relays_min_count = self.opts.min_peer_relays_count
        else:
            self.peer_relays_min_count = max(
                gateway_constants.MIN_PEER_RELAYS_BY_COUNTRY[self.opts.country],
                gateway_constants.MIN_PEER_RELAYS_BY_COUNTRY[best_relay_country]
            )

        best_relay_peers = best_relay_peers[:self.peer_relays_min_count]

        logger.trace("Best relay peers to node {} are: {}", self.opts.node_id, best_relay_peers)

        return best_relay_peers

    def _register_potential_relay_peers(self, best_relay_peers: List[OutboundPeerModel]) -> None:
        best_relay_set = set(best_relay_peers)

        to_disconnect = self.peer_relays - best_relay_set
        to_connect = best_relay_set - self.peer_relays

        if len(to_connect) > 0:
            logger.debug("Sending relay switch notification to SDN.")
            self.requester.send_threaded_request(
                sdn_http_service.submit_gateway_switching_relays_event,
                self.opts.node_id
            )

        for peer_to_remove in to_disconnect:
            logger.info("Disconnecting from current relay {}", peer_to_remove)
            self.peer_relays.remove(peer_to_remove)
            if self.opts.split_relays:
                self.peer_transaction_relays = {
                    peer for peer in self.peer_transaction_relays
                    if not (
                        peer.ip == peer_to_remove.ip and peer.port == peer_to_remove.port + 1
                    )
                }

        for peer_to_connect in to_connect:
            logger.info("Connecting to better relay {}", peer_to_connect)
            self.peer_relays.add(peer_to_connect)
            if self.opts.split_relays:
                self.peer_transaction_relays.add(
                    OutboundPeerModel(
                        peer_to_connect.ip,
                        peer_to_connect.port + 1,
                        f"{peer_to_connect.node_id}-tx",
                        False,
                        peer_to_connect.attributes,
                        NodeType.RELAY_TRANSACTION
                    )
                )

        self.on_updated_peers(self._get_all_peers())
        self._schedule_fetch_relays_from_sdn(
            gateway_constants.RELAY_CONNECTION_REEVALUATION_INTERVAL_S
        )

    def _tracked_block_cleanup(self) -> float:
        tx_service = self.get_tx_service()
        block_queuing_service = self.block_queuing_service_manager.get_designated_block_queuing_service()
        if block_queuing_service is not None:
            tracked_blocks_to_clean = []
            tracked_blocks = tx_service.get_oldest_tracked_block(0)
            recent_blocks = list(block_queuing_service.iterate_recent_block_hashes())
            for depth, block_hash in enumerate(recent_blocks):
                if depth > self.network.block_confirmations_count and block_hash in tracked_blocks:
                    self.block_cleanup_service.block_cleanup_request(block_hash)
                    tracked_blocks_to_clean.append(block_hash)
            logger.trace(
                "tracked block cleanup, request cleanup of {} blocks: {}, tracked blocks: {} recent blocks: {}",
                len(tracked_blocks_to_clean), tracked_blocks_to_clean, tracked_blocks, recent_blocks)
        else:
            logger.warning(log_messages.TRACKED_BLOCK_CLEANUP_ERROR, "block queuing service is not available")
        return self.tracked_block_cleanup_interval_s

    def _process_remote_blockchain_peer_from_sdn(self, get_remote_blockchain_peer_future: Future):
        try:
            remote_blockchain_peer = get_remote_blockchain_peer_future.result()
            if remote_blockchain_peer is None:
                logger.debug("Did not receive expected remote blockchain peer from BDN. Retrying.")
                self.alarm_queue.register_alarm(
                    gateway_constants.REMOTE_BLOCKCHAIN_SDN_CONTACT_RETRY_SECONDS,
                    self.send_request_for_remote_blockchain_peer
                )
            else:
                logger.debug("Processing remote blockchain peer: {}", remote_blockchain_peer)
                self.on_updated_remote_blockchain_peer(remote_blockchain_peer)
        except Exception as e:
            logger.info("Got {} when trying to read remote blockchain peer from sdn", e)

    def _schedule_fetch_relays_from_sdn(
        self,
        delay: int = constants.SDN_CONTACT_RETRY_SECONDS
    ) -> None:
        """
        Cancels existing tasks for fetching relays from SDN (usually the reevaluation interval)
        and schedules a new one.
        """
        check_relay_alarm_id = self.check_relay_alarm_id
        if check_relay_alarm_id is not None:
            self.alarm_queue.unregister_alarm(check_relay_alarm_id)

        self.check_relay_alarm_id = self.alarm_queue.register_alarm(
            delay, self.sync_and_send_request_for_relay_peers, self.network_num
        )

    def _clear_transaction_service(self) -> None:
        logger.debug("Clearing all data in transaction service.")
        self._tx_service.clear()

    def _check_memory_threshold(self):
        if self.opts.should_restart_on_high_memory and \
                memory_utils.get_app_memory_usage() > gateway_constants.CHECK_MEMORY_THRESHOLD_LIMIT:
            logger.warning(log_messages.NODE_EXCEEDS_MEMORY)
            self.should_force_exit = True
            self.should_restart_on_high_memory = True
        return gateway_constants.CHECK_MEMORY_THRESHOLD_INTERVAL_S
