import asyncio
import time
from abc import ABCMeta, abstractmethod
from argparse import Namespace
from typing import Tuple, Optional, ClassVar, Type, Set, List, Iterable

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.models.node_type import NodeType
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.models.blockchain_network_model import BlockchainNetworkModel
from bxcommon.models.node_event_model import NodeEventType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.network.network_direction import NetworkDirection
from bxcommon.network.peer_info import ConnectionPeerInfo
from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxcommon.services import sdn_http_service
from bxcommon.services.broadcast_service import BroadcastService
from bxcommon.services.transaction_service import TransactionService
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.utils import network_latency, memory_utils, convert
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import hooks
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway.rpc.gateway_rpc_server import GatewayRpcServer

from bxgateway import gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.services.block_recovery_service import BlockRecoveryService
from bxgateway.services.gateway_broadcast_service import GatewayBroadcastService
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.utils import configuration_utils
from bxgateway.utils import node_cache
from bxgateway.utils.blockchain_message_queue import BlockchainMessageQueue
from bxgateway.utils.logging.status import status_log
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxutils import logging
from bxutils.services.node_ssl_service import NodeSSLService

logger = logging.get_logger(__name__)


class AbstractGatewayNode(AbstractNode):
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.
    """

    __metaclass__ = ABCMeta

    NODE_TYPE = NodeType.EXTERNAL_GATEWAY
    RELAY_CONNECTION_CLS: ClassVar[Type[AbstractRelayConnection]] = None

    node_conn: Optional[AbstractGatewayBlockchainConnection] = None
    remote_blockchain_ip: Optional[str] = None
    remote_blockchain_port: Optional[int] = None
    remote_node_conn: Optional[AbstractGatewayBlockchainConnection] = None
    _preferred_gateway_connection: Optional[GatewayConnection] = None

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
    block_queuing_service: BlockQueuingService
    block_processing_service: BlockProcessingService
    block_cleanup_service: AbstractBlockCleanupService
    _tx_service: TransactionService

    _block_from_node_handling_times: ExpiringDict[Sha256Hash, int]
    _block_from_bdn_handling_times: ExpiringDict[Sha256Hash, Tuple[int, str]]

    def __init__(self, opts: Namespace, node_ssl_service: Optional[NodeSSLService] = None):
        super(AbstractGatewayNode, self).__init__(opts, node_ssl_service)
        if opts.split_relays:
            opts.peer_transaction_relays = [
                OutboundPeerModel(peer_relay.ip, peer_relay.port + 1, node_type=NodeType.RELAY_TRANSACTION)
                for peer_relay in opts.peer_relays
            ]
            opts.outbound_peers += opts.peer_transaction_relays
        else:
            opts.peer_transaction_relays = []

        self.peer_gateways = set(opts.peer_gateways)
        self.peer_relays = set(opts.peer_relays)
        self.peer_transaction_relays = set(opts.peer_transaction_relays)

        self.node_msg_queue = BlockchainMessageQueue(opts.blockchain_message_ttl)
        self.remote_node_msg_queue = BlockchainMessageQueue(opts.remote_blockchain_message_ttl)

        self.blocks_seen = ExpiringSet(self.alarm_queue, gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)
        self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)
        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)
        self.neutrality_service = NeutralityService(self)
        self.block_queuing_service = self.build_block_queuing_service()
        self.block_processing_service = BlockProcessingService(self)
        self.block_cleanup_service = self.build_block_cleanup_service()

        self.send_request_for_relay_peers_num_of_calls = 0
        if not self.opts.peer_relays:
            self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS, self.send_request_for_relay_peers)

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
        self.init_node_config_update()

        self._block_from_node_handling_times = ExpiringDict(self.alarm_queue,
                                                            gateway_constants.BLOCK_HANDLING_TIME_EXPIRATION_TIME_S)
        self._block_from_bdn_handling_times = ExpiringDict(self.alarm_queue,
                                                           gateway_constants.BLOCK_HANDLING_TIME_EXPIRATION_TIME_S)

        self.schedule_blockchain_liveliness_check(self.opts.initial_liveliness_check)
        self.schedule_relay_liveliness_check(self.opts.initial_liveliness_check)

        self.opts.has_fully_updated_tx_service = False
        self.last_sync_message_received_by_network[self.network_num] = time.time()

        self.block_cleanup_processed_blocks = ExpiringSet(self.alarm_queue,
                                                          gateway_constants.BLOCK_CONFIRMATION_EXPIRE_TIME_S)

        self.message_converter = None
        self.account_id: Optional[str] = None
        self._rpc_server = GatewayRpcServer(self)

        status_log.initialize(self.opts.use_extensions, self.opts.source_version, self.opts.external_ip,
                              self.opts.continent, self.opts.country)

    @abstractmethod
    def build_blockchain_connection(
            self, socket_connection: SocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    @abstractmethod
    def build_relay_connection(self, socket_connection: SocketConnectionProtocol) -> AbstractRelayConnection:
        pass

    @abstractmethod
    def build_remote_blockchain_connection(
            self, socket_connection: SocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    @abstractmethod
    def build_block_queuing_service(self) -> BlockQueuingService:
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

    def get_preferred_gateway_connection(self):
        """
        Gets gateway connection of highest priority. This is usually a bloxroute owned node, but can also be
        overridden by the command line arguments, otherwise a randomly chosen connection.

        This can return None.
        """
        if self._preferred_gateway_connection is None or not self._preferred_gateway_connection.is_alive():

            connected_opts_peer = self._find_active_connection(self.opts.peer_gateways)
            if connected_opts_peer is not None:
                self._preferred_gateway_connection = connected_opts_peer
                return connected_opts_peer

            bloxroute_peers = filter(lambda peer: peer.is_internal_gateway, self.peer_gateways)
            connected_bloxroute_peer = self._find_active_connection(bloxroute_peers)
            if connected_bloxroute_peer is not None:
                self._preferred_gateway_connection = connected_bloxroute_peer
                return connected_bloxroute_peer

            connected_peer = self._find_active_connection(self.peer_gateways)
            if connected_peer is not None:
                self._preferred_gateway_connection = connected_peer
                return connected_peer

            self._preferred_gateway_connection = None

        return self._preferred_gateway_connection

    def set_preferred_gateway_connection(self, connection):
        """
        Override current preferred gateway connection.
        """
        if not isinstance(connection, GatewayConnection):
            raise ValueError("Cannot set preferred gateway connection to a connection of type {}"
                             .format(type(connection)))
        self._preferred_gateway_connection = connection

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
        try:
            await self._rpc_server.start()
        except Exception as e:
            logger.error("Failed to initialize BDN RPC server: {}.", e, exc_info=True)

    def close(self):
        loop = asyncio.get_event_loop()
        try:
            # TODO: convert to proper async
            loop.run_until_complete(self._rpc_server.stop())
        except Exception as e:
            logger.error("Failed to close BDN RPC server: {}.", e, exc_info=True)
        super(AbstractGatewayNode, self).close()

    def _register_potential_relay_peers(self, potential_relay_peers: List[OutboundPeerModel]):
        logger.trace("Potential relay peers: {}", [node.node_id for node in potential_relay_peers])
        best_relay_peer = network_latency.get_best_relay_by_ping_latency(potential_relay_peers)
        if not best_relay_peer:
            best_relay_peer = potential_relay_peers[0]
        logger.trace("Best relay peer to node {} is: {}", self.opts.node_id, best_relay_peer)

        if best_relay_peer.ip != self.opts.external_ip or best_relay_peer.port != self.opts.external_port:
            self.peer_relays.add(best_relay_peer)
            # Split relay mode always starts a transaction relay at one port number higher than the block relay
            if self.opts.split_relays:
                self.peer_transaction_relays.add(OutboundPeerModel(
                    best_relay_peer.ip,
                    best_relay_peer.port + 1,
                    "{}-tx".format(best_relay_peer.node_id),
                    False,
                    best_relay_peer.attributes,
                    node_type=NodeType.RELAY_TRANSACTION
                ))
        self.on_updated_peers(self._get_all_peers())

    def send_request_for_relay_peers(self):
        """
        Requests potential relay peers from SDN. Merges list with provided command line relays.
        """
        potential_relay_peers = sdn_http_service.fetch_potential_relay_peers_by_network(self.opts.node_id,
                                                                                        self.network_num)

        if potential_relay_peers:
            node_cache.update(self.opts, potential_relay_peers)
            self._register_potential_relay_peers(potential_relay_peers)
        else:
            # if called too many times to send_request_for_relay_peers, reset relay peers to empty list in cache file
            if self.send_request_for_relay_peers_num_of_calls > gateway_constants.SEND_REQUEST_RELAY_PEERS_MAX_NUM_OF_CALLS:
                node_cache.update(self.opts, [])
                self.send_request_for_relay_peers_num_of_calls = 0
            self.send_request_for_relay_peers_num_of_calls += 1

            cache_file_info = node_cache.read(self.opts)
            if cache_file_info is not None:
                cache_file_relay_peers = cache_file_info.relay_peers
                if cache_file_relay_peers:
                    self._register_potential_relay_peers(cache_file_relay_peers)
                else:
                    return constants.SDN_CONTACT_RETRY_SECONDS
            else:
                return constants.SDN_CONTACT_RETRY_SECONDS

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
                IpEndpoint(self.remote_blockchain_ip, self.remote_blockchain_port),
                ConnectionType.REMOTE_BLOCKCHAIN_NODE)
            )
        return peers

    def build_connection(self, socket_connection: SocketConnectionProtocol) -> Optional[AbstractConnection]:
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
                                               self.opts.external_ip, self.opts.continent, self.opts.country)
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
        if self.node_conn is not None:
            self.node_conn.enqueue_msg(msg)
        else:
            logger.trace("Adding message to local node's message queue: {}", msg)
            self.node_msg_queue.append(msg)

    def send_msg_to_remote_node(self, msg: AbstractMessage):
        """
        Sends a message to remote connected blockchain node.
        """
        if self.remote_node_conn is not None:
            self.remote_node_conn.enqueue_msg(msg)
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
            self.node_conn.peer_ip,
            self.node_conn.peer_port
        )

    def on_blockchain_connection_destroyed(self, connection: AbstractGatewayBlockchainConnection):
        self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_event,
                                             NodeEventType.BLOCKCHAIN_NODE_CONN_ERR,
                                             self.opts.node_id,
                                             connection.peer_ip,
                                             connection.peer_port)
        self.node_conn = None
        self.node_msg_queue.pop_items()
        self.schedule_blockchain_liveliness_check(self.opts.stay_alive_duration)

    def on_remote_blockchain_connection_ready(self, connection: AbstractGatewayBlockchainConnection):
        for msg in self.remote_node_msg_queue.pop_items():
            connection.enqueue_msg(msg)
        self.remote_node_conn = connection
        self.requester.send_threaded_request(
            sdn_http_service.submit_peer_connection_event,
            NodeEventType.REMOTE_BLOCKCHAIN_CONN_ESTABLISHED,
            self.opts.node_id,
            self.remote_node_conn.peer_ip,
            self.remote_node_conn.peer_port
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
                                               self.opts.external_ip, self.opts.continent, self.opts.country)
        if connection_type == ConnectionType.GATEWAY:
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_error_event,
                                                 self.opts.node_id,
                                                 ip,
                                                 port)
            self._remove_gateway_peer(ip, port)
        elif connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            self.send_request_for_remote_blockchain_peer()
        elif connection_type & ConnectionType.RELAY_BLOCK:
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_error_event,
                                                 self.opts.node_id,
                                                 ip,
                                                 port)
            self._remove_relay_peer(ip, port)
        elif self.opts.split_relays and connection_type & ConnectionType.RELAY_TRANSACTION:
            self.requester.send_threaded_request(sdn_http_service.submit_peer_connection_error_event,
                                                 self.opts.node_id,
                                                 ip,
                                                 port)
            self._remove_relay_transaction_peer(ip, port)

    def on_updated_remote_blockchain_peer(self, outbound_peer):
        self.remote_blockchain_ip = outbound_peer.ip
        self.remote_blockchain_port = outbound_peer.port
        self.enqueue_connection(outbound_peer.ip, outbound_peer.port, ConnectionType.REMOTE_BLOCKCHAIN_NODE)

    def on_block_seen_by_blockchain_node(self, block_hash: Sha256Hash):
        self.blocks_seen.add(block_hash)
        recovery_canceled = self.block_recovery_service.cancel_recovery_for_block(block_hash)
        if recovery_canceled:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECOVERY_CANCELED,
                                                      network_num=self.network_num)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash)

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
            logger.error("Gateway does not have an active connection to the blockchain node. "
                         "Check that the blockchain node is running and available. Exiting.")

    def check_relay_liveliness(self):
        if not self.connection_pool.get_by_connection_type(ConnectionType.RELAY_ALL):
            self.should_force_exit = True
            logger.error("Gateway does not have an active connection to the relay network. "
                         "There may be issues with the BDN. Exiting.")

    def _send_request_for_gateway_peers(self):
        """
        Requests gateway peers from SDN. Merges list with provided command line gateways.
        """
        peer_gateways = sdn_http_service.fetch_gateway_peers(self.opts.node_id)
        if not peer_gateways and not self.peer_gateways and \
            self.send_request_for_gateway_peers_num_of_calls < gateway_constants.SEND_REQUEST_GATEWAY_PEERS_MAX_NUM_OF_CALLS:
            # Try again later
            logger.warning("Did not receive expected gateway peers from BDN. Retrying.")
            self.send_request_for_gateway_peers_num_of_calls += 1
            if self.send_request_for_gateway_peers_num_of_calls == \
                gateway_constants.SEND_REQUEST_GATEWAY_PEERS_MAX_NUM_OF_CALLS:
                logger.warning("Giving up on querying for gateway peers from the BDN.")
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
                            if peer_relay.ip != ip and peer_relay.port != port}
        if self.opts.split_relays:
            if self.connection_pool.has_connection(ip, port + 1):
                transaction_connection = self.connection_pool.get_by_ipport(ip, port + 1)
                logger.debug("Removing relay transaction connection matching block relay host: {}",
                             transaction_connection)
                transaction_connection.mark_for_close(False)
            self._remove_relay_transaction_peer(ip, port + 1, False)

        self.outbound_peers = self._get_all_peers()
        if len(self.peer_relays) < gateway_constants.MIN_PEER_RELAYS:
            self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS,
                                            self.send_request_for_relay_peers)
            self.schedule_relay_liveliness_check(gateway_constants.DEFAULT_STAY_ALIVE_DURATION_S)

    def _remove_relay_transaction_peer(self, ip: str, port: int, remove_block_relay: bool = True):
        """
        Clean up relay transactions peer on connection failure. (after giving up retry)
        Destroys matching block relay.
        :param ip: transaction relay peer ip
        :param port: transaction relay peer port
        :param remove_block_relay: if to remove the corresponding block relay (to avoid infinite recursing)
        """
        self.peer_transaction_relays = {peer_relay for peer_relay in self.peer_transaction_relays
                                        if peer_relay.ip != ip and peer_relay.port != port}
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

    def _sync_tx_services(self):
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
                return constants.TX_SERVICE_SYNC_PROGRESS_S
        else:
            self.on_fully_updated_tx_service()

    def _transaction_sync_timeout(self):
        if not self.opts.has_fully_updated_tx_service:
            logger.warning("Gateway transaction sync took too long; marking gateway as synced.")
            self.alarm_queue.unregister_alarm(self._check_sync_relay_connections_alarm_id)
            self.on_fully_updated_tx_service()
            return constants.CANCEL_ALARMS

    def _check_sync_relay_connections(self):
        if self.network_num in self.last_sync_message_received_by_network and \
                time.time() - self.last_sync_message_received_by_network[self.network_num] > constants.LAST_MSG_FROM_RELAY_THRESHOLD_S:
            logger.warning(
                "It has been more than {0} seconds since the last time gateway received a message from requested "
                "relay, assuming requested relay turned offline and mark gateway as synced",
                constants.LAST_MSG_FROM_RELAY_THRESHOLD_S)
            self.last_sync_message_received_by_network.pop(self.network_num, None)
            self.alarm_queue.unregister_alarm(self._transaction_sync_timeout_alarm_id)
            self.on_fully_updated_tx_service()
