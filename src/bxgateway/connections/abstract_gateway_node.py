from abc import ABCMeta, abstractmethod
from collections import deque
from bxcommon import constants
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.node_event_model import NodeEventType
from bxcommon.services import sdn_http_service
from bxcommon.services.transaction_service import TransactionService
from bxcommon.storage.block_encrypted_cache import BlockEncryptedCache
from bxcommon.utils import logger
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway import gateway_constants
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.services.block_recovery_service import BlockRecoveryService
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxgateway.utils.network_latency import get_best_relay_by_ping_latency


class AbstractGatewayNode(AbstractNode):
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.

    Attributes
    ----------
    opts: node configuration options
    peer_gateways: gateway nodes that is/will be connected to
    peer_relays: relay nodes that is/will be connected to
    node_conn: connection object to blockchain node
    in_progress_blocks: hash => (key, encrypted_block) dict of blocks for which keys
                        have not yet been released to the network
    block_recovery_service: service for finding unknown transaction short ids
    _tx_service: service for managing transaction short ids
    """

    __metaclass__ = ABCMeta

    NODE_TYPE = NodeType.GATEWAY
    RELAY_CONNECTION_CLS = None

    def __init__(self, opts):
        super(AbstractGatewayNode, self).__init__(opts)
        self.opts = opts
        self.peer_gateways = set(opts.peer_gateways)
        self.peer_relays = set(opts.peer_relays)
        self.node_conn = None  # Connection object for the blockchain node
        self.node_msg_queue = deque()
        self.blocks_seen = ExpiringSet(self.alarm_queue, gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)

        self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)

        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)
        self.neutrality_service = NeutralityService(self)
        self.block_queuing_service = BlockQueuingService(self)
        self.block_processing_service = BlockProcessingService(self)

        if not self.opts.peer_relays:
            self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS, self.send_request_for_relay_peers)

        self.remote_blockchain_ip = None
        self.remote_blockchain_port = None
        if opts.connect_to_remote_blockchain:
            if opts.remote_blockchain_peer is not None:
                self.remote_blockchain_ip = opts.remote_blockchain_ip
                self.remote_blockchain_port = opts.remote_blockchain_port
                self.enqueue_connection(opts.remote_blockchain_ip, opts.remote_blockchain_port)
            else:
                # offset SDN calls so all the peers aren't queued up at the same time
                self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS + 1,
                                                self.send_request_for_remote_blockchain_peer)
        self.remote_node_conn = None
        self.remote_node_msg_queue = deque()

        # offset SDN calls so all the peers aren't queued up at the same time
        self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS + 2, self._send_request_for_gateway_peers)
        self._tx_service = TransactionService(self, self.network_num)

        self._preferred_gateway_connection = None

        self.init_transaction_stat_logging()

    @abstractmethod
    def get_blockchain_connection_cls(self):
        pass

    @abstractmethod
    def get_relay_connection_cls(self):
        pass

    @abstractmethod
    def get_remote_blockchain_connection_cls(self):
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

    def send_request_for_relay_peers(self):
        """
        Requests potential relay peers from SDN. Merges list with provided command line relays.
        """
        potential_relay_peers = sdn_http_service.fetch_potential_relay_peers(self.opts.node_id)
        best_relay_match = []
        if len(potential_relay_peers) != 0:
            logger.debug("Potential relay peers: {}".
                         format(", ".join([node.node_id for node in potential_relay_peers])))

            best_relay_peer = get_best_relay_by_ping_latency(potential_relay_peers)
            if not best_relay_peer:
                best_relay_peer = potential_relay_peers[0]
            best_relay_match.append(best_relay_peer)

        logger.debug("Best relay peer to node {} is: {}".format(self.opts.node_id, best_relay_match))
        self._add_relay_peers(set(best_relay_match))
        self.on_updated_peers(self._get_all_peers())

        # Try again later.
        if not potential_relay_peers:
            return constants.SDN_CONTACT_RETRY_SECONDS

    def _send_request_for_gateway_peers(self):
        """
        Requests gateway peers from SDN. Merges list with provided command line gateways.
        """
        peer_gateways = sdn_http_service.fetch_gateway_peers(self.opts.node_id)
        logger.trace("Processing updated peer gateways: {}".format(peer_gateways))
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

    def get_connection_class(self, ip=None, port=None, from_me=False):
        if self.is_local_blockchain_address(ip, port):
            return self.get_blockchain_connection_cls()
        elif self.remote_blockchain_ip == ip and self.remote_blockchain_port == port:
            return self.get_remote_blockchain_connection_cls()
        # only other gateways attempt to actively connect to gateways
        elif not from_me or any(ip == peer_gateway.ip and port == peer_gateway.port
                                for peer_gateway in self.peer_gateways):
            return GatewayConnection
        elif any(ip == peer_relay.ip and port == peer_relay.port for peer_relay in self.peer_relays):
            return self.get_relay_connection_cls()
        else:
            logger.error("Attempted connection to peer that's not a blockchain, remote blockchain, gateway, or relay. "
                         "Tried: {}:{}, from_me={}. Ignoring.".format(ip, port, from_me))
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

    def destroy_conn(self, conn, retry_connection=False):
        if not retry_connection and conn.CONNECTION_TYPE == ConnectionType.GATEWAY:
            self._remove_gateway_peer(conn.peer_ip, conn.peer_port)
        if not retry_connection and conn.CONNECTION_TYPE == ConnectionType.RELAY:
            self._remove_relay_peer(conn.peer_ip, conn.peer_port)
        if retry_connection and conn.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE:
            sdn_http_service.submit_peer_connection_event(NodeEventType.BLOCKCHAIN_NODE_CONN_ERR, self.opts.node_id,
                                                          conn.peer_ip, conn.peer_port)
        super(AbstractGatewayNode, self).destroy_conn(conn, retry_connection)

    def on_failed_connection_retry(self, ip, port, connection_type):
        if connection_type == ConnectionType.GATEWAY:
            sdn_http_service.submit_peer_connection_error_event(self.opts.node_id, ip, port)
            self._remove_gateway_peer(ip, port)
        elif connection_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            self.send_request_for_remote_blockchain_peer()
        elif connection_type == ConnectionType.RELAY:
            sdn_http_service.submit_peer_connection_error_event(self.opts.node_id, ip, port)
            self._remove_relay_peer(ip, port)

        return 0

    def on_connection_initialized(self, fileno):
        super(AbstractGatewayNode, self).on_connection_initialized(fileno)

        conn = self.connection_pool.get_by_fileno(fileno)
        if conn and conn.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE:
            sdn_http_service.submit_peer_connection_event(NodeEventType.BLOCKCHAIN_NODE_CONN_ESTABLISHED,
                                                          self.opts.node_id, conn.peer_ip, conn.peer_port)

    def should_retry_connection(self, ip, port, connection_type):
        return (super(AbstractGatewayNode, self).should_retry_connection(ip, port, connection_type)
                or OutboundPeerModel(ip, port) in self.opts.peer_gateways
                or connection_type == ConnectionType.BLOCKCHAIN_NODE
                or (ip == self.opts.remote_blockchain_ip and port == self.opts.remote_blockchain_port))

    def _get_all_peers(self):
        return list(self.peer_gateways.union(self.peer_relays))

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

    def _add_relay_peers(self, relay_peers):
        for relay_peer in relay_peers:
            if relay_peer.ip != self.opts.external_ip or relay_peer.port != self.opts.external_port:
                self.peer_relays.add(relay_peer)

    def _remove_relay_peer(self, ip, port):
        relay_to_remove = None
        for peer_relay in self.peer_relays:
            if ip == peer_relay.ip and port == peer_relay.port:
                relay_to_remove = peer_relay
                break

        if relay_to_remove is not None:
            self.peer_relays.remove(relay_to_remove)
            self.outbound_peers = self._get_all_peers()
            if len(self.peer_relays) < gateway_constants.MIN_PEER_RELAYS:
                self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS,
                                                self.send_request_for_relay_peers)

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


