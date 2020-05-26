import asyncio
import time
from abc import ABCMeta, abstractmethod
from argparse import Namespace
from concurrent.futures import Future
from typing import Tuple, Optional, ClassVar, Type, Set, List, Iterable, cast

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.models.blockchain_network_model import BlockchainNetworkModel
from bxcommon.models.node_event_model import NodeEventType
from bxcommon.models.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.network.network_direction import NetworkDirection
from bxcommon.network.peer_info import ConnectionPeerInfo
from bxcommon.services import sdn_http_service
from bxcommon.services.broadcast_service import BroadcastService
from bxcommon.services.transaction_service import TransactionService
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.utils import network_latency, memory_utils, convert, node_cache
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import hooks
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxgateway import log_messages
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxcommon.rpc import rpc_constants
from bxgateway.rpc.gateway_rpc_server import GatewayRpcServer
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.block_recovery_service import BlockRecoveryService
from bxgateway.services.gateway_broadcast_service import GatewayBroadcastService
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.utils import configuration_utils
from bxgateway.utils.blockchain_message_queue import BlockchainMessageQueue
from bxgateway.utils.logging.status import status_log
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxutils import logging
from bxutils.services.node_ssl_service import NodeSSLService
from bxutils.ssl.extensions import extensions_factory
from bxutils.ssl.ssl_certificate_type import SSLCertificateType

logger = logging.get_logger(__name__)


class AbstractGatewayNode(AbstractNode):
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.
    """

    __metaclass__ = ABCMeta

    NODE_TYPE = NodeType.EXTERNAL_GATEWAY
    # pyre-fixme[8]: Attribute has type `Type[AbstractRelayConnection]`; used as `None`.
    RELAY_CONNECTION_CLS: ClassVar[Type[AbstractRelayConnection]] = None

    node_conn: Optional[AbstractGatewayBlockchainConnection] = None
    remote_blockchain_ip: Optional[str] = None
    remote_blockchain_port: Optional[int] = None
    remote_node_conn: Optional[AbstractGatewayBlockchainConnection] = None

    _blockchain_liveliness_alarm: Optional[AlarmId] = None
    _relay_liveliness_alarm: Optional[AlarmId] = None

    node_msg_queue: BlockchainMessageQueue
    remote_node_msg_queue: BlockchainMessageQueue

    peer_gateways: Set[OutboundPeerModel]
    # if opts.split_relays is set, then this set contains only block relays
    peer_relays: Set[OutboundPeerModel]
    peer_transaction_relays: Set[OutboundPeerModel]
    blocks_seen: ExpiringSet
    in_progress_blocks: BlockEncryptedCache
    block_recovery_service: BlockRecoveryService
    block_queuing_service: AbstractBlockQueuingService
    block_processing_service: BlockProcessingService
    block_cleanup_service: AbstractBlockCleanupService
    _tx_service: TransactionService

    _block_from_node_handling_times: ExpiringDict[Sha256Hash, float]
    _block_from_bdn_handling_times: ExpiringDict[Sha256Hash, Tuple[float, str]]

    tracked_block_cleanup_interval_s: float

    def __init__(self, opts: Namespace, node_ssl_service: NodeSSLService,
                 tracked_block_cleanup_interval_s=constants.CANCEL_ALARMS):
        super(AbstractGatewayNode, self).__init__(
            opts, node_ssl_service)
        if opts.split_relays:
            opts.peer_transaction_relays = [
                OutboundPeerModel(peer_relay.ip, peer_relay.port + 1, node_type=NodeType.RELAY_TRANSACTION)
                for peer_relay in opts.peer_relays
            ]
            opts.outbound_peers += opts.peer_transaction_relays
        else:
            opts.peer_transaction_relays = []

        self.quota_level = 0
        self.last_quota_level_notification_time = 0.0
        self.peer_gateways = set(opts.peer_gateways)
        self.peer_relays = set(opts.peer_relays)
        self.peer_transaction_relays = set(opts.peer_transaction_relays)
        self.peer_relays_min_count = 1

        self.node_msg_queue = BlockchainMessageQueue(opts.blockchain_message_ttl)
        self.remote_node_msg_queue = BlockchainMessageQueue(opts.remote_blockchain_message_ttl)

        self.blocks_seen = ExpiringSet(
            self.alarm_queue,
            gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S,
            "gateway_blocks_seen"
        )
        self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)
        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)
        self.neutrality_service = NeutralityService(self)
        self.block_queuing_service = self.build_block_queuing_service()
        self.block_processing_service = BlockProcessingService(self)
        self.block_cleanup_service = self.build_block_cleanup_service()

        self.send_request_for_relay_peers_num_of_calls = 0
        if not self.opts.peer_relays:
            self.check_relay_alarm_id = self.alarm_queue.register_alarm(
                constants.SDN_CONTACT_RETRY_SECONDS,
                self.send_request_for_relay_peers
            )
        else:
            self.check_relay_alarm_id = self.alarm_queue.register_alarm(
                gateway_constants.RELAY_CONNECTION_REEVALUATION_INTERVAL_S,
                self.send_request_for_relay_peers
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
        self.network = self._get_blockchain_network()

        if opts.use_extensions:
            from bxcommon.services.extension_transaction_service import ExtensionTransactionService
            self._tx_service = ExtensionTransactionService(self, self.network_num)
        else:
            self._tx_service = TransactionService(self, self.network_num)

        self.init_transaction_stat_logging()
        self.init_bdn_performance_stats_logging()
        self.init_node_config_update()

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

        self.schedule_blockchain_liveliness_check(self.opts.initial_liveliness_check)
        self.schedule_relay_liveliness_check(self.opts.initial_liveliness_check)

        self.opts.has_fully_updated_tx_service = False
        self.alarm_queue.register_alarm(constants.TX_SERVICE_SYNC_PROGRESS_S, self.sync_tx_services)

        self.block_cleanup_processed_blocks = ExpiringSet(
            self.alarm_queue,
            gateway_constants.BLOCK_CONFIRMATION_EXPIRE_TIME_S,
            "gateway_block_cleanup_processed_blocks"
        )

        self.message_converter: Optional[AbstractMessageConverter] = None
        self.account_id: Optional[str] = extensions_factory.get_account_id(
            node_ssl_service.get_certificate(SSLCertificateType.PRIVATE)
        )
        self.default_tx_quota_type: QuotaType = opts.default_tx_quota_type
        if self.default_tx_quota_type == QuotaType.PAID_DAILY_QUOTA and self.account_id is None:
            logger.error(log_messages.INVALID_ACCOUNT_ID)
            self.default_tx_quota_type = QuotaType.FREE_DAILY_QUOTA
        self._rpc_server = GatewayRpcServer(self)

        self.tracked_block_cleanup_interval_s = tracked_block_cleanup_interval_s
        if self.tracked_block_cleanup_interval_s > 0:
            self.alarm_queue.register_alarm(self.tracked_block_cleanup_interval_s, self._tracked_block_cleanup,
                                            alarm_name="tracked_blocks_cleanup")

        status_log.initialize(self.opts.use_extensions, self.opts.source_version, self.opts.external_ip,
                              self.opts.continent, self.opts.country, self.opts.should_update_source_version,
                              self.account_id, self.quota_level)

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

    @abstractmethod
    def build_block_queuing_service(self) -> AbstractBlockQueuingService:
        pass

    @abstractmethod
    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        pass

    def get_broadcast_service(self) -> BroadcastService:
        return GatewayBroadcastService(self.connection_pool)

    def init_transaction_stat_logging(self):
        gateway_transaction_stats_service.set_node(self)
        self.alarm_queue.register_alarm(gateway_transaction_stats_service.interval,
                                        gateway_transaction_stats_service.flush_info)

    def init_bdn_performance_stats_logging(self):
        gateway_bdn_performance_stats_service.set_node(self)
        self.alarm_queue.register_alarm(gateway_bdn_performance_stats_service.interval,
                                        self.send_bdn_performance_stats)

    def send_bdn_performance_stats(self) -> int:
        relay_connections = self.connection_pool.get_by_connection_type(ConnectionType.RELAY_BLOCK)
        if not relay_connections:
            gateway_bdn_performance_stats_service.create_interval_data_object()
            return gateway_bdn_performance_stats_service.interval

        relay_connection = cast(AbstractRelayConnection, next(iter(relay_connections)))
        gateway_bdn_performance_stats_service.close_interval_data()
        # pyre-ignore
        relay_connection.send_bdn_performance_stats(gateway_bdn_performance_stats_service.interval_data)
        gateway_bdn_performance_stats_service.create_interval_data_object()
        return gateway_bdn_performance_stats_service.interval

    def init_node_config_update(self):
        self.update_node_config()
        self.alarm_queue.register_alarm(constants.ALARM_QUEUE_INIT_EVENT, self.update_node_config)

    def update_node_config(self):
        configuration_utils.update_node_config(self)
        return self.opts.config_update_interval

    def record_mem_stats(self):
        self._tx_service.log_tx_service_mem_stats()
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
        return super(AbstractGatewayNode, self).record_mem_stats()

    def get_tx_service(self, network_num=None):
        if network_num is not None and network_num != self.opts.blockchain_network_num:
            raise ValueError("Gateway is running with network number '{}' but tx service for '{}' was requested"
                             .format(self.opts.blockchain_network_num, network_num))

        return self._tx_service

    def on_fully_updated_tx_service(self):
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
        try:
            await asyncio.wait_for(self._rpc_server.start(), rpc_constants.RPC_SERVER_INIT_TIMEOUT_S)
        except Exception as e:
            logger.error(log_messages.RPC_INITIALIZATION_FAIL, e, exc_info=True)

    async def close(self):
        try:
            await asyncio.wait_for(self._rpc_server.stop(), rpc_constants.RPC_SERVER_STOP_TIMEOUT_S)
        except Exception as e:
            logger.error(log_messages.RPC_CLOSE_FAIL, e, exc_info=True)
        await super(AbstractGatewayNode, self).close()

    def send_request_for_relay_peers(self):
        """
        Requests potential relay peers from SDN. Merges list with provided command line relays.

        This function retrieves from the SDN potential_relay_peers_by_network
        Then it try to ping for each relay (timeout of 2 seconds). The ping is done in parallel
        Once there are ping result, it calculate the best relay and decides if need to switch relays

        The above can take time, so the functions is splitted into several internal functions and use the thread pool
        not to block the main thread.
        """

        self.check_relay_alarm_id = None
        self.requester.send_threaded_request(
            sdn_http_service.fetch_potential_relay_peers_by_network,
            self.opts.node_id,
            self.network_num,
            done_callback=self._process_potential_relays_from_sdn
        )

        return constants.CANCEL_ALARMS

    def send_request_for_remote_blockchain_peer(self):
        """
        Requests a bloxroute owned blockchain node from the SDN.
        """
        remote_blockchain_peer = sdn_http_service.fetch_remote_blockchain_peer(self.opts.node_id)
        if remote_blockchain_peer is None:
            logger.debug("Did not receive expected remote blockchain peer from BDN. Retrying.")
            self.alarm_queue.register_alarm(gateway_constants.REMOTE_BLOCKCHAIN_SDN_CONTACT_RETRY_SECONDS,
                                            self.send_request_for_remote_blockchain_peer)
        else:
            logger.debug("Processing remote blockchain peer: {}", remote_blockchain_peer)
            return self.on_updated_remote_blockchain_peer(remote_blockchain_peer)

    def get_outbound_peer_info(self) -> List[ConnectionPeerInfo]:
        peers = [ConnectionPeerInfo(
            IpEndpoint(peer.ip, peer.port),
            convert.peer_node_to_connection_type(self.NODE_TYPE, peer.node_type))
            for peer in self.outbound_peers
        ]
        peers.append(ConnectionPeerInfo(
            IpEndpoint(self.opts.blockchain_ip, self.opts.blockchain_port), ConnectionType.BLOCKCHAIN_NODE
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
                                               self.opts.should_update_source_version, self.account_id, self.quota_level)
        if self.is_local_blockchain_address(ip, port):
            return self.build_blockchain_connection(socket_connection)
        elif self.remote_blockchain_ip == ip and self.remote_blockchain_port == port:
            return self.build_remote_blockchain_connection(socket_connection)
        # only other gateways attempt to actively connect to gateways
        elif not from_me or any(ip == peer_gateway.ip and port == peer_gateway.port
                                for peer_gateway in self.peer_gateways):
            return GatewayConnection(socket_connection, self)
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_relays):
            relay_connection = self.build_relay_connection(socket_connection)
            if self.opts.split_relays:
                relay_connection.CONNECTION_TYPE = ConnectionType.RELAY_BLOCK
                relay_connection.disable_buffering()
            return relay_connection
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_transaction_relays):
            assert self.opts.split_relays
            relay_connection = self.build_relay_connection(socket_connection)
            relay_connection.CONNECTION_TYPE = ConnectionType.RELAY_TRANSACTION
            return relay_connection
        else:
            logger.debug("Attempted connection to peer that's not a blockchain, remote blockchain, gateway, or relay. "
                         "Tried: {}:{}, from_me={}. Ignoring.", ip, port, from_me)
            return None

    def is_local_blockchain_address(self, ip, port):
        return ip == self.opts.blockchain_ip and port == self.opts.blockchain_port

    def send_msg_to_node(self, msg: AbstractMessage):
        """
        Sends a message to the blockchain node this is connected to.
        """
        node_conn = self.node_conn
        if node_conn is not None:
            node_conn.enqueue_msg(msg)
        else:
            logger.trace("Adding message to local node's message queue: {}", msg)
            self.node_msg_queue.append(msg)

    def send_msg_to_remote_node(self, msg: AbstractMessage):
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
        return (super(AbstractGatewayNode, self).continue_retrying_connection(ip, port, connection_type)
                or OutboundPeerModel(ip, port) in self.opts.peer_gateways
                or connection_type == ConnectionType.BLOCKCHAIN_NODE
                or (connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE and
                    self.num_retries_by_ip[(ip, port)] < gateway_constants.REMOTE_BLOCKCHAIN_MAX_CONNECT_RETRIES))

    def on_blockchain_connection_ready(self, connection: AbstractGatewayBlockchainConnection):
        for msg in self.node_msg_queue.pop_items():
            connection.enqueue_msg(msg)

        self.node_conn = connection
        self.cancel_blockchain_liveliness_check()
        self.requester.send_threaded_request(
            sdn_http_service.submit_peer_connection_event,
            NodeEventType.BLOCKCHAIN_NODE_CONN_ESTABLISHED,
            self.opts.node_id,
            connection.peer_ip,
            connection.peer_port
        )

    def on_blockchain_connection_destroyed(self, connection: AbstractGatewayBlockchainConnection):
        self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_event,
                                             NodeEventType.BLOCKCHAIN_NODE_CONN_ERR,
                                             self.opts.node_id,
                                             connection.peer_ip,
                                             connection.peer_port,
                                             connection.get_connection_state_details())
        self.node_conn = None
        self.node_msg_queue.pop_items()

    def log_refused_connection(self, peer_info: ConnectionPeerInfo, error: str):
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

    def on_remote_blockchain_connection_ready(self, connection: AbstractGatewayBlockchainConnection):
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

    def on_remote_blockchain_connection_destroyed(self, connection: AbstractGatewayBlockchainConnection):
        self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_event,
                                             NodeEventType.REMOTE_BLOCKCHAIN_CONN_ERR,
                                             self.opts.node_id,
                                             connection.peer_ip,
                                             connection.peer_port)
        self.remote_node_conn = None
        self.remote_node_msg_queue.pop_items()

    def on_relay_connection_ready(self):
        self.cancel_relay_liveliness_check()

    def on_failed_connection_retry(self, ip: str, port: int, connection_type: ConnectionType) -> None:
        self.alarm_queue.register_approx_alarm(2 * constants.MIN_SLEEP_TIMEOUT, constants.MIN_SLEEP_TIMEOUT,
                                               status_log.update_alarm_callback, self.connection_pool,
                                               self.opts.use_extensions, self.opts.source_version,
                                               self.opts.external_ip, self.opts.continent, self.opts.country,
                                               self.opts.should_update_source_version, self.account_id, self.quota_level)
        if connection_type in ConnectionType.GATEWAY:
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_error_event,
                                                 self.opts.node_id,
                                                 ip,
                                                 port)
            self._remove_gateway_peer(ip, port)
        elif connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            self.send_request_for_remote_blockchain_peer()
        elif ConnectionType.RELAY_BLOCK in connection_type:
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_error_event,
                                                 self.opts.node_id,
                                                 ip,
                                                 port)
            self._remove_relay_peer(ip, port)
        elif self.opts.split_relays and ConnectionType.RELAY_TRANSACTION in connection_type:
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_error_event,
                                                 self.opts.node_id,
                                                 ip,
                                                 port)
            self._remove_relay_transaction_peer(ip, port)

        # Reset number of retries in case if SDN instructs to connect to the same node again
        self.num_retries_by_ip[(ip, port)] = 0

    def on_updated_remote_blockchain_peer(self, outbound_peer):
        self.remote_blockchain_ip = outbound_peer.ip
        self.remote_blockchain_port = outbound_peer.port
        self.enqueue_connection(outbound_peer.ip, outbound_peer.port, ConnectionType.REMOTE_BLOCKCHAIN_NODE)

    def on_block_seen_by_blockchain_node(
            self,
            block_hash: Sha256Hash,
            block_message: Optional[AbstractBlockMessage] = None
    ):
        self.blocks_seen.add(block_hash)
        recovery_canceled = self.block_recovery_service.cancel_recovery_for_block(block_hash)
        if recovery_canceled:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECOVERY_CANCELED,
                                                      network_num=self.network_num)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash,
            block_message
        )

        self.log_blocks_network_content(self.network_num, block_message)

    def log_blocks_network_content(self, network_num: int, block_msg) -> None:
        pass

    def post_block_cleanup_tasks(
        self,
        block_hash: Sha256Hash,
        short_ids: Iterable[int],
        unknown_tx_hashes: Iterable[Sha256Hash]):
        """post cleanup tasks for blocks, override method to implement"""
        pass

    def schedule_blockchain_liveliness_check(self, time_from_now_s: int):
        if not self._blockchain_liveliness_alarm and self.opts.require_blockchain_connection:
            self._blockchain_liveliness_alarm = self.alarm_queue.register_alarm(time_from_now_s,
                                                                                self.check_blockchain_liveliness)

    def schedule_relay_liveliness_check(self, time_from_now_s: int):
        if not self._relay_liveliness_alarm:
            self._relay_liveliness_alarm = self.alarm_queue.register_alarm(time_from_now_s,
                                                                           self.check_relay_liveliness)

    def cancel_blockchain_liveliness_check(self):
        if self._blockchain_liveliness_alarm:
            self.alarm_queue.unregister_alarm(self._blockchain_liveliness_alarm)
            self._blockchain_liveliness_alarm = None

    def cancel_relay_liveliness_check(self):
        if self._relay_liveliness_alarm:
            self.alarm_queue.unregister_alarm(self._relay_liveliness_alarm)
            self._relay_liveliness_alarm = None

    def check_blockchain_liveliness(self):
        """
        Checks that the gateway has functional connections to the blockchain node.

        Closes the gateway if it does not, as the gateway is not providing any useful functionality.
        """
        if self.node_conn is None:
            self.should_force_exit = True
            logger.error(log_messages.NO_ACTIVE_BLOCKCHAIN_CONNECTION)

    def check_relay_liveliness(self):
        if not self.connection_pool.get_by_connection_type(ConnectionType.RELAY_ALL):
            self.should_force_exit = True
            logger.error(log_messages.NO_ACTIVE_BDN_CONNECTIONS)

    def should_process_block_hash(self, block_hash: Optional[Sha256Hash] = None) -> bool:
        result = True
        reason = None

        if not self.opts.has_fully_updated_tx_service:
            result = False
            reason = "Tx sync in progress"

        node_conn = self.node_conn
        if node_conn is None or not node_conn.is_active():
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

    def _send_request_for_gateway_peers(self):
        """
        Requests gateway peers from SDN. Merges list with provided command line gateways.
        """
        peer_gateways = sdn_http_service.fetch_gateway_peers(self.opts.node_id)
        if not peer_gateways and not self.peer_gateways and \
            self.send_request_for_gateway_peers_num_of_calls < gateway_constants.SEND_REQUEST_GATEWAY_PEERS_MAX_NUM_OF_CALLS:
            # Try again later
            logger.warning(log_messages.NO_GATEWAY_PEERS)
            self.send_request_for_gateway_peers_num_of_calls += 1
            if self.send_request_for_gateway_peers_num_of_calls == \
                gateway_constants.SEND_REQUEST_GATEWAY_PEERS_MAX_NUM_OF_CALLS:
                logger.warning(log_messages.ABANDON_GATEWAY_PEER_REQUEST)
            return constants.SDN_CONTACT_RETRY_SECONDS
        else:
            logger.debug("Processing updated peer gateways: {}", peer_gateways)
            self._add_gateway_peers(peer_gateways)
            self.on_updated_peers(self._get_all_peers())
            self.send_request_for_gateway_peers_num_of_calls = 0

    def _get_all_peers(self):
        return list(self.peer_gateways.union(self.peer_relays).union(self.peer_transaction_relays))

    def _add_gateway_peers(self, gateways_peers):
        for gateway_peer in gateways_peers:
            if gateway_peer.ip != self.opts.external_ip or gateway_peer.port != self.opts.external_port:
                self.peer_gateways.add(gateway_peer)

    def _remove_gateway_peer(self, ip, port):
        gateway_to_remove = None
        for peer_gateway in self.peer_gateways:
            if ip == peer_gateway.ip and port == peer_gateway.port:
                gateway_to_remove = peer_gateway
                break

        if gateway_to_remove is not None:
            self.peer_gateways.remove(gateway_to_remove)
            self.outbound_peers = self._get_all_peers()
            if len(self.peer_gateways) < self.opts.min_peer_gateways:
                self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS,
                                                self._send_request_for_gateway_peers)

    def _remove_relay_peer(self, ip: str, port: int):
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
            self._remove_relay_transaction_peer(ip, port + 1, False)

        self.outbound_peers = self._get_all_peers()

        if (
            len(self.peer_relays) < self.peer_relays_min_count
            or len(self.peer_transaction_relays) < self.peer_relays_min_count and
            self.check_relay_alarm_id is None
        ):
            logger.debug("Removed relay peer with ip {} and port {}. "
                         "Current number of relay peers is lower than required. "
                         "Scheduling request of new relays from BDN.",
                         ip, port)
            self.check_relay_alarm_id = self.alarm_queue.register_alarm(
                constants.SDN_CONTACT_RETRY_SECONDS, self.send_request_for_relay_peers
            )

    def _remove_relay_transaction_peer(self, ip: str, port: int, remove_block_relay: bool = True):
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
            self._remove_relay_peer(ip, port - 1)

    def _find_active_connection(self, outbound_peers):
        for peer in outbound_peers:
            if self.connection_pool.has_connection(peer.ip, peer.port):
                connection = self.connection_pool.get_by_ipport(peer.ip, peer.port)
                if connection.is_active():
                    return connection
        return None

    def _get_blockchain_network(self) -> BlockchainNetworkModel:
        for network in self.opts.blockchain_networks:
            if network.network_num == self.network_num:
                return network
        raise EnvironmentError(f"Unexpectedly did not find network num {self.network} in set of blockchain networks: "
                               f"{self.opts.blockchain_networks}")

    def sync_tx_services(self):
        logger.info("Starting to sync transaction state with BDN.")
        super(AbstractGatewayNode, self).sync_tx_services()
        if self.opts.sync_tx_service:
            retry = True
            if self.opts.split_relays:
                relay_tx_connections = self.connection_pool.get_by_connection_type(ConnectionType.RELAY_TRANSACTION)
                relay_block_connections = self.connection_pool.get_by_connection_type(ConnectionType.RELAY_BLOCK)

                if relay_tx_connections and relay_block_connections:
                    relay_tx_connection = next(iter(relay_tx_connections))
                    relay_block_connection = next(iter(relay_block_connections))

                    if relay_tx_connection.is_active() and relay_block_connection.is_active():
                        relay_tx_connection.send_tx_service_sync_req(self.network_num)
                        relay_block_connection.send_tx_service_sync_req(self.network_num)
                        retry = False
            else:
                relay_connections = self.connection_pool.get_by_connection_type(ConnectionType.RELAY_ALL)
                if relay_connections:
                    relay_connection = next(iter(relay_connections))
                    if relay_connection.is_active():
                        relay_connection.send_tx_service_sync_req(self.network_num)
                        retry = False

            if retry:
                logger.info("Relay connection is not ready to sync transaction state with BDN. Scheduling retry.")
                return constants.TX_SERVICE_SYNC_PROGRESS_S
        else:
            self.on_fully_updated_tx_service()

    def _transaction_sync_timeout(self):
        if not self.opts.has_fully_updated_tx_service:
            logger.warning(log_messages.TX_SYNC_TIMEOUT)
            self.alarm_queue.unregister_alarm(self._check_sync_relay_connections_alarm_id)
            self.on_fully_updated_tx_service()
            return constants.CANCEL_ALARMS

    def _check_sync_relay_connections(self):
        if self.network_num in self.last_sync_message_received_by_network and \
            time.time() - self.last_sync_message_received_by_network[self.network_num] > \
            constants.LAST_MSG_FROM_RELAY_THRESHOLD_S:
            logger.warning(log_messages.RELAY_CONNECTION_TIMEOUT,
                           constants.LAST_MSG_FROM_RELAY_THRESHOLD_S)

            self.on_network_synced(self.network_num)
            self.alarm_queue.unregister_alarm(self._transaction_sync_timeout_alarm_id)
            self.on_fully_updated_tx_service()

    def _process_potential_relays_from_sdn(self, get_potential_relays_future: Future):
        """
        part of send_request_for_relay_peers logic
        This function is called once there is a result from SDN regarding potential_relays.
        The process of potential_relays should be run in the main thread, hence the register_alarm
        :param get_potential_relays_future: the list of potentital relays from the BDN API
        :return:
        """

        try:
            potential_relay_peers = get_potential_relays_future.result()

            if potential_relay_peers:
                node_cache.update(self.opts, potential_relay_peers)
            else:
                # if called too many times to send_request_for_relay_peers, reset relay peers to empty list in cache file
                if self.send_request_for_relay_peers_num_of_calls > gateway_constants.SEND_REQUEST_RELAY_PEERS_MAX_NUM_OF_CALLS:
                    node_cache.update(self.opts, [])
                    self.send_request_for_relay_peers_num_of_calls = 0
                self.send_request_for_relay_peers_num_of_calls += 1

                cache_file_info = node_cache.read(self.opts)
                if cache_file_info is not None:
                    potential_relay_peers = cache_file_info.relay_peers

                if not potential_relay_peers:
                    if self.check_relay_alarm_id is None:
                        self.check_relay_alarm_id = self.alarm_queue.register_alarm(
                            constants.SDN_CONTACT_RETRY_SECONDS,
                            self.send_request_for_relay_peers
                        )
                    return

            # check the network latency using the thread pool
            self.requester.send_threaded_request(
                # pyre-fixme[6]: Expected `(...) -> None` for 1st param but got
                #  `BoundMethod[typing.Callable(AbstractGatewayNode._find_best_relay_peers)[[Named(self,
                #  AbstractGatewayNode), Named(potential_relay_peers,
                #  List[OutboundPeerModel])], List[OutboundPeerModel]],
                #  AbstractGatewayNode]`.
                self._find_best_relay_peers,
                potential_relay_peers,
                done_callback=self._register_potential_relay_peers_from_future
            )
        except Exception as e:
            logger.info("Got {} when trying to read potential_relays from sdn", e)

        if self.check_relay_alarm_id is None:
            self.check_relay_alarm_id = self.alarm_queue.register_alarm(
                gateway_constants.RELAY_CONNECTION_REEVALUATION_INTERVAL_S,
                self.send_request_for_relay_peers
            )

    def _find_best_relay_peers(self, potential_relay_peers: List[OutboundPeerModel]) -> \
        List[OutboundPeerModel]:
        logger.info("Received list of potential relays from BDN: {}.",
                    ", ".join([node.ip for node in potential_relay_peers]))

        logger.trace("Potential relay peers: {}", [node.node_id for node in potential_relay_peers])

        best_relay_peers = network_latency.get_best_relays_by_ping_latency_one_per_country(
            potential_relay_peers, gateway_constants.MAX_PEER_RELAYS_COUNT
        )

        if not best_relay_peers:
            best_relay_peers = potential_relay_peers

        best_relay_country = best_relay_peers[0].get_country()

        self.peer_relays_min_count = max(gateway_constants.MIN_PEER_RELAYS_BY_COUNTRY[self.opts.country],
                                         gateway_constants.MIN_PEER_RELAYS_BY_COUNTRY[best_relay_country])

        best_relay_peers = best_relay_peers[:self.peer_relays_min_count]

        logger.trace("Best relay peers to node {} are: {}", self.opts.node_id, best_relay_peers)

        return best_relay_peers

    def _register_potential_relay_peers_from_future(self, ping_potential_relays_future: Future):
        best_relay_peers = ping_potential_relays_future.result()
        self._register_potential_relay_peers(best_relay_peers)

    def _register_potential_relay_peers(self, best_relay_peers: List[OutboundPeerModel]):
        best_relay_set = set(best_relay_peers)

        to_disconnect = self.peer_relays - best_relay_set
        to_connect = best_relay_set - self.peer_relays

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

    def _tracked_block_cleanup(self):
        tx_service = self.get_tx_service()
        block_queuing_service = self.block_queuing_service
        if self.block_queuing_service is not None:
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
            logger.warning("tracked block cleanup failed, block queuing service is not available")
        return self.tracked_block_cleanup_interval_s
