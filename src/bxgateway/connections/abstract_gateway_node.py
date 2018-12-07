from abc import ABCMeta, abstractmethod
from collections import deque

from bxcommon import constants
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.services import sdn_http_service
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import logger
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.services.block_recovery_service import BlockRecoveryService
from bxgateway.storage.block_encrypted_cache import BlockEncryptedCache
from bxgateway.testing.lossy_relay_connection import LossyRelayConnection
from bxgateway.testing.test_modes import TestModes
from bxgateway.testing.unencrypted_block_cache import UnencryptedCache


class AbstractGatewayNode(AbstractNode):
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.

    Attributes
    ----------
    opts: node configuration options
    idx: node index, included in HelloMessages 
    peer_gateways: gateway nodes that is/will be connected to
    peer_relays: relay nodes that is/will be connected to
    node_conn: connection object to blockchain node
    in_progress_blocks: hash => (key, encrypted_block) dict of blocks for which keys
                        have not yet been released to the network
    block_recovery_service: service for finding unknown transaction short ids
    """

    __metaclass__ = ABCMeta

    node_type = NodeType.GATEWAY

    relay_connection_cls = None

    def __init__(self, opts):
        super(AbstractGatewayNode, self).__init__(opts)

        self.opts = opts
        self.idx = constants.NULL_IDX
        self.peer_gateways = set(opts.peer_gateways)
        self.peer_relays = set(opts.peer_relays)

        self.node_conn = None  # Connection object for the blockchain node
        self.node_msg_queue = deque()
        self.blocks_seen = ExpiringSet(self.alarm_queue, constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)

        if TestModes.DISABLE_ENCRYPTION in self.opts.test_mode:
            self.in_progress_blocks = UnencryptedCache(self.alarm_queue)
        else:
            self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)

        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)

        self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS, self._send_request_for_gateway_peers)
        self._tx_service = TransactionService(self)
        self.network_num = opts.network_num

    def get_tx_service(self, network_num=None):
        if network_num is not None and network_num != self.opts.network_num:
            raise ValueError("Gateway is running with network number '{}' but tx service for '{}' was requested"
                             .format(self.opts.network_num, network_num))

        return self._tx_service

    def send_request_for_relay_peers(self):
        """
        Requests relay peers from SDN. Merges list with provided command line relays.
        """
        peer_relays = sdn_http_service.fetch_relay_peers(self.opts.node_id)
        self.peer_relays = set(self.opts.peer_relays + peer_relays)
        self.on_updated_peers(self._get_all_peers())

        # Try again later.
        if not peer_relays:
            return constants.SDN_CONTACT_RETRY_SECONDS

    def _send_request_for_gateway_peers(self):
        """
        Requests gateway peers from SDN. Merges list with provided command line gateways.
        """
        peer_gateways = sdn_http_service.fetch_gateway_peers(self.opts.node_id)
        self._add_gateway_peers(peer_gateways)
        self.on_updated_peers(self._get_all_peers())

        # Try again later
        if not peer_gateways:
            return constants.SDN_CONTACT_RETRY_SECONDS

    def get_outbound_peer_addresses(self):
        peers = [(peer.ip, peer.port) for peer in self.outbound_peers]
        peers.append((self.opts.blockchain_ip, self.opts.blockchain_port))
        return peers

    def get_connection_class(self, ip=None, port=None, from_me=False):
        if self.opts.blockchain_ip == ip and self.opts.blockchain_port == port:
            return self.get_blockchain_connection_cls()
        # only other gateways attempt to actively connect to gateways
        elif not from_me or any(ip == peer_gateway.ip and port == peer_gateway.port
                                for peer_gateway in self.peer_gateways):
            return GatewayConnection
        else:
            return self.get_relay_connection_cls()

    def is_blockchain_node_address(self, ip, port):
        return ip == self.opts.blockchain_ip and port == self.opts.blockchain_port

    def send_msg_to_node(self, msg):
        """
        Sends a message to the blockchain node this is connected to.
        """
        if self.node_conn is not None:
            logger.debug("Sending message to node: " + repr(msg))
            self.node_conn.enqueue_msg(msg)
        else:
            logger.debug("Adding things to node's message queue")
            self.node_msg_queue.append(msg)

    def destroy_conn(self, conn, retry_connection=False):
        if not retry_connection and conn.connection_type == ConnectionType.GATEWAY:
            self._remove_gateway_peer(conn.peer_ip, conn.peer_port)
        super(AbstractGatewayNode, self).destroy_conn(conn, retry_connection)

    def retry_init_client_socket(self, ip, port, connection_type):
        super(AbstractGatewayNode, self).retry_init_client_socket(ip, port, connection_type)
        if connection_type == ConnectionType.GATEWAY and not self.should_retry_connection(ip, port, connection_type):
            del self.num_retries_by_ip[ip]
            logger.debug("Not retrying connection to {0}:{1}- maximum connections exceeded!".format(ip, port))
            sdn_http_service.submit_peer_connection_error_event(self.opts.node_id, ip, port)

            self._remove_gateway_peer(ip, port)

        return 0

    def should_retry_connection(self, ip, port, connection_type):
        return super(AbstractGatewayNode, self).should_retry_connection(ip, port, connection_type) or \
               OutboundPeerModel(ip, port) in self.opts.peer_gateways

    def _get_all_peers(self):
        return list(self.peer_gateways.union(self.peer_relays))

    def _add_gateway_peers(self, gateways_peers):
        for gateway_peer in gateways_peers:
            if gateway_peer.ip != self.opts.external_ip or gateway_peer.port != self.opts.external_port:
                self.peer_gateways.add(gateway_peer)

    def _remove_gateway_peer(self, ip, port):
        outbound_peer = OutboundPeerModel(ip, port)
        if outbound_peer in self.peer_gateways:
            self.peer_gateways.remove(OutboundPeerModel(ip, port))
            self.on_updated_peers(self._get_all_peers())
            if len(self.peer_gateways) < self.opts.min_peer_gateways:
                self.alarm_queue.register_alarm(constants.SDN_CONTACT_RETRY_SECONDS,
                                                self._send_request_for_gateway_peers)

    @abstractmethod
    def get_blockchain_connection_cls(self):
        pass

    @abstractmethod
    def get_relay_connection_cls(self):
        pass

    def _get_relay_connection_cls(self):
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            return LossyRelayConnection
        else:
            return self.relay_connection_cls
