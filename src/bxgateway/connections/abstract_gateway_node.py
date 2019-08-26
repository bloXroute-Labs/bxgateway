import time
from abc import ABCMeta, abstractmethod
from argparse import Namespace
from collections import deque
from typing import Tuple, Optional, ClassVar, Type, Set, Deque, List

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.node_type import NodeType
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.models.node_event_model import NodeEventType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.services import sdn_http_service
from bxcommon.services.transaction_service import TransactionService
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.utils import logger, network_latency
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.services.block_recovery_service import BlockRecoveryService
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.utils import node_cache
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service


class AbstractGatewayNode(AbstractNode):
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.
    """

    __metaclass__ = ABCMeta

    NODE_TYPE = NodeType.GATEWAY
    RELAY_CONNECTION_CLS: ClassVar[Type[AbstractRelayConnection]] = None

    node_conn: Optional[AbstractGatewayBlockchainConnection] = None
    node_msg_queue: Deque[AbstractMessage] = deque()
    remote_blockchain_ip: Optional[str] = None
    remote_blockchain_port: Optional[int] = None
    remote_node_conn: Optional[AbstractGatewayBlockchainConnection] = None
    remote_node_msg_queue: Deque[AbstractMessage] = deque()
    _preferred_gateway_connection: Optional[GatewayConnection] = None

    peer_gateways: Set[OutboundPeerModel]
    # if opts.split_relays is set, then this set contains only block relays
    peer_relays: Set[OutboundPeerModel]
    peer_transaction_relays: Set[OutboundPeerModel]
    blocks_seen: ExpiringSet
    in_progress_blocks: BlockEncryptedCache
    block_recovery_service: BlockRecoveryService
    block_queuing_service: BlockQueuingService
    block_processing_service: BlockProcessingService
    _tx_service: TransactionService

    def __init__(self, opts: Namespace):
        super(AbstractGatewayNode, self).__init__(opts)
        if opts.split_relays:
            opts.peer_transaction_relays = [
                OutboundPeerModel(peer_relay.ip, peer_relay.port + 1)
                for peer_relay in opts.peer_relays
            ]
            opts.outbound_peers += opts.peer_transaction_relays
        else:
            opts.peer_transaction_relays = []

        self.peer_gateways = set(opts.peer_gateways)
        self.peer_relays = set(opts.peer_relays)
        self.peer_transaction_relays = set(opts.peer_transaction_relays)

        self.blocks_seen = ExpiringSet(self.alarm_queue, gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)
        self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)
        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)
        self.neutrality_service = NeutralityService(self)
        self.block_queuing_service = self.build_block_queuing_service()
        self.block_processing_service = BlockProcessingService(self)

        self.send_request_for_relay_peers_num_of_calls = 0
        if not self.opts.peer_relays:
            self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS, self.send_request_for_relay_peers)

        if opts.connect_to_remote_blockchain:
            if opts.remote_blockchain_peer is not None:
                self.remote_blockchain_ip = opts.remote_blockchain_ip
                self.remote_blockchain_port = opts.remote_blockchain_port
                self.enqueue_connection(opts.remote_blockchain_ip, opts.remote_blockchain_port)
            else:
                # offset SDN calls so all the peers aren't queued up at the same time
                self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS + 1,
                                                self.send_request_for_remote_blockchain_peer)

        # offset SDN calls so all the peers aren't queued up at the same time
        self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS + 2, self._send_request_for_gateway_peers)
        if opts.use_extensions:
            from bxcommon.services.extension_transaction_service import ExtensionTransactionService
            self._tx_service = ExtensionTransactionService(self, self.network_num)
        else:
            self._tx_service = TransactionService(self, self.network_num)

        self.init_transaction_stat_logging()

        self._block_from_node_handling_times: ExpiringDict[Sha256Hash, int] = ExpiringDict(self.alarm_queue,
                                                                                           gateway_constants.BLOCK_HANDLING_TIME_EXPIRATION_TIME_S)
        self._block_from_bdn_handling_times: ExpiringDict[Sha256Hash, Tuple[int, str]] = ExpiringDict(self.alarm_queue,
                                                                                                      gateway_constants.BLOCK_HANDLING_TIME_EXPIRATION_TIME_S)

    @abstractmethod
    def build_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                    from_me: bool) -> AbstractGatewayBlockchainConnection:
        pass

    @abstractmethod
    def build_relay_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                               from_me: bool) -> AbstractRelayConnection:
        pass

    @abstractmethod
    def build_remote_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                           from_me: bool) -> AbstractGatewayBlockchainConnection:
        pass

    @abstractmethod
    def build_block_queuing_service(self) -> BlockQueuingService:
        pass

    def get_tx_service(self, network_num=None):
        if network_num is not None and network_num != self.opts.blockchain_network_num:
            raise ValueError("Gateway is running with network number '{}' but tx service for '{}' was requested"
                             .format(self.opts.blockchain_network_num, network_num))

        return self._tx_service

    def get_preferred_gateway_connection(self):
        """
        Gets gateway connection of highest priority. This is usually a bloxroute owned node, but can also be
        overridden by the command line arguments, otherwise a randomly chosen connection.

        This can return None.
        """
        if self._preferred_gateway_connection is None or \
                self._preferred_gateway_connection.state != ConnectionState.ESTABLISHED:

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

    def _register_potential_relay_peers(self, potential_relay_peers: List[OutboundPeerModel]):
        logger.debug("Potential relay peers: {}", [node.node_id for node in potential_relay_peers])
        best_relay_peer = network_latency.get_best_relay_by_ping_latency(potential_relay_peers)
        if not best_relay_peer:
            best_relay_peer = potential_relay_peers[0]
        logger.debug("Best relay peer to node {} is: {}", self.opts.node_id, best_relay_peer)

        if best_relay_peer.ip != self.opts.external_ip or best_relay_peer.port != self.opts.external_port:
            self.peer_relays.add(best_relay_peer)
            # Split relay mode always starts a transaction relay at one port number higher than the block relay
            if self.opts.split_relays:
                self.peer_transaction_relays.add(OutboundPeerModel(best_relay_peer.ip, best_relay_peer.port + 1,
                                                                   "{}-tx".format(best_relay_peer.node_id), False,
                                                                   best_relay_peer.attributes))
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

    def _send_request_for_gateway_peers(self):
        """
        Requests gateway peers from SDN. Merges list with provided command line gateways.
        """
        peer_gateways = sdn_http_service.fetch_gateway_peers(self.opts.node_id)
        logger.trace("Processing updated peer gateways: {}", peer_gateways)
        self._add_gateway_peers(peer_gateways)
        self.on_updated_peers(self._get_all_peers())

        # Try again later
        if not peer_gateways:
            return constants.SDN_CONTACT_RETRY_SECONDS

    def send_request_for_remote_blockchain_peer(self):
        """
        Requests a bloxroute owned blockchain node from the SDN.
        """
        remote_blockchain_peer = sdn_http_service.fetch_remote_blockchain_peer(self.opts.blockchain_network_num)
        if remote_blockchain_peer is None:
            logger.trace("Did not receive expected remote blockchain peer. Retrying.".format(remote_blockchain_peer))
            return constants.SDN_CONTACT_RETRY_SECONDS
        else:
            logger.trace("Processing remote blockchain peer: {}".format(remote_blockchain_peer))
            return self.on_updated_remote_blockchain_peer(remote_blockchain_peer)

    def on_updated_remote_blockchain_peer(self, outbound_peer):
        self.remote_blockchain_ip = outbound_peer.ip
        self.remote_blockchain_port = outbound_peer.port
        self.enqueue_connection(outbound_peer.ip, outbound_peer.port)

    def get_outbound_peer_addresses(self):
        peers = [(peer.ip, peer.port) for peer in self.outbound_peers]
        peers.append((self.opts.blockchain_ip, self.opts.blockchain_port))
        if self.remote_blockchain_ip is not None and self.remote_blockchain_port is not None:
            peers.append((self.remote_blockchain_ip, self.remote_blockchain_port))
        return peers

    def build_connection(self, socket_connection: SocketConnection, ip: str, port: int, from_me: bool = False) \
            -> Optional[AbstractConnection]:
        """
        Builds a connection class object based on the characteristics of the ip, port, and direction of the connection.

        For split relays, force shadowing override of the connection's `CONNECTION_TYPE` attribute. This isn't ideal,
        but prevents the necessity of writing many types of connection classes that differ only by the `CONNECTION_TYPE`
        attribute and lots of branching conditional logic.

        :param socket_connection: socket connection accepting or initializing connection
        :param ip:  connection ip
        :param port: connection port
        :param from_me: if the connection was initiated by local node
        :return: connection object, if connection class is found
        """
        if self.is_local_blockchain_address(ip, port):
            return self.build_blockchain_connection(socket_connection, (ip, port), from_me)
        elif self.remote_blockchain_ip == ip and self.remote_blockchain_port == port:
            return self.build_remote_blockchain_connection(socket_connection, (ip, port), from_me)
        # only other gateways attempt to actively connect to gateways
        elif not from_me or any(ip == peer_gateway.ip and port == peer_gateway.port
                                for peer_gateway in self.peer_gateways):
            return GatewayConnection(socket_connection, (ip, port), self, from_me)
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_relays):
            relay_connection = self.build_relay_connection(socket_connection, (ip, port), from_me)
            if self.opts.split_relays:
                relay_connection.CONNECTION_TYPE = ConnectionType.RELAY_BLOCK
                relay_connection.disable_buffering()
            return relay_connection
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_transaction_relays):
            assert self.opts.split_relays
            relay_connection = self.build_relay_connection(socket_connection, (ip, port), from_me)
            relay_connection.CONNECTION_TYPE = ConnectionType.RELAY_TRANSACTION
            return relay_connection
        else:
            logger.error("Attempted connection to peer that's not a blockchain, remote blockchain, gateway, or relay. "
                         "Tried: {}:{}, from_me={}. Ignoring.", ip, port, from_me)
            return None

    def is_local_blockchain_address(self, ip, port):
        return ip == self.opts.blockchain_ip and port == self.opts.blockchain_port

    def send_msg_to_node(self, msg):
        """
        Sends a message to the blockchain node this is connected to.
        """
        if self.node_conn is not None:
            self.node_conn.enqueue_msg(msg)
        else:
            logger.trace("Adding message to local node's message queue: {}".format(msg))
            self.node_msg_queue.append(msg.rawbytes())

    def send_msg_to_remote_node(self, msg):
        """
        Sends a message to remote connected blockchain node.
        """
        if self.remote_node_conn is not None:
            self.remote_node_conn.enqueue_msg(msg)
        else:
            logger.debug("Adding message to remote node's message queue: {}".format(msg))
            self.remote_node_msg_queue.append(msg.rawbytes())

    def should_retry_connection(self, ip: str, port: int, connection_type: ConnectionType) -> bool:
        return (super(AbstractGatewayNode, self).should_retry_connection(ip, port, connection_type)
                or OutboundPeerModel(ip, port) in self.opts.peer_gateways
                or connection_type == ConnectionType.BLOCKCHAIN_NODE
                or (ip == self.opts.remote_blockchain_ip and port == self.opts.remote_blockchain_port))

    def destroy_conn(self, conn, retry_connection=False):
        if conn.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE:
            sdn_http_service.submit_peer_connection_event(NodeEventType.BLOCKCHAIN_NODE_CONN_ERR, self.opts.node_id,
                                                          conn.peer_ip, conn.peer_port)
        elif conn.CONNECTION_TYPE == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            sdn_http_service.submit_peer_connection_event(NodeEventType.REMOTE_BLOCKCHAIN_CONN_ERR, self.opts.node_id,
                                                          conn.peer_ip, conn.peer_port)

        super(AbstractGatewayNode, self).destroy_conn(conn, retry_connection)

    def on_failed_connection_retry(self, ip: str, port: int, connection_type: ConnectionType) -> None:
        if connection_type == ConnectionType.GATEWAY:
            sdn_http_service.submit_peer_connection_error_event(self.opts.node_id, ip, port)
            self._remove_gateway_peer(ip, port)
        elif connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            self.send_request_for_remote_blockchain_peer()
        elif connection_type & ConnectionType.RELAY_BLOCK:
            sdn_http_service.submit_peer_connection_error_event(self.opts.node_id, ip, port)
            self._remove_relay_peer(ip, port)
        elif self.opts.split_relays and connection_type & ConnectionType.RELAY_TRANSACTION:
            sdn_http_service.submit_peer_connection_error_event(self.opts.node_id, ip, port)
            self._remove_relay_transaction_peer(ip, port)

    def on_connection_initialized(self, fileno):
        super(AbstractGatewayNode, self).on_connection_initialized(fileno)

        conn = self.connection_pool.get_by_fileno(fileno)

        if conn and conn.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE:
            sdn_http_service.submit_peer_connection_event(NodeEventType.BLOCKCHAIN_NODE_CONN_ESTABLISHED,
                                                          self.opts.node_id, conn.peer_ip, conn.peer_port)
        elif conn and conn.CONNECTION_TYPE == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            sdn_http_service.submit_peer_connection_event(NodeEventType.REMOTE_BLOCKCHAIN_CONN_ESTABLISHED,
                                                          self.opts.node_id, conn.peer_ip, conn.peer_port)

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
                logger.info("Removing relay transaction connection matching block relay host: {}",
                            transaction_connection)
                self.destroy_conn(transaction_connection, False)
            self._remove_relay_transaction_peer(ip, port + 1, False)

        self.outbound_peers = self._get_all_peers()
        if len(self.peer_relays) < gateway_constants.MIN_PEER_RELAYS:
            self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS,
                                            self.send_request_for_relay_peers)

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
            logger.info("Removing relay block connection matching transaction relay host: {}",
                        block_connection)
            self.destroy_conn(block_connection, False)

    def _find_active_connection(self, outbound_peers):
        for peer in outbound_peers:
            if self.connection_pool.has_connection(peer.ip, peer.port):
                connection = self.connection_pool.get_by_ipport(peer.ip, peer.port)
                if connection.is_active():
                    return connection
        return None

    def record_mem_stats(self):
        self._tx_service.log_tx_service_mem_stats()
        return super(AbstractGatewayNode, self).record_mem_stats()

    def init_transaction_stat_logging(self):
        gateway_transaction_stats_service.set_node(self)
        self.alarm_queue.register_alarm(gateway_transaction_stats_service.interval,
                                        gateway_transaction_stats_service.flush_info)

    def on_block_seen_by_blockchain_node(self, block_hash: Sha256Hash):
        self.blocks_seen.add(block_hash)
        self.block_recovery_service.cancel_recovery_for_block(block_hash)
        self.block_queuing_service.mark_block_seen_by_blockchain_node(block_hash)
