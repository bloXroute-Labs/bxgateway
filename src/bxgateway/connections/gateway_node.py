from collections import deque

from bxcommon import constants
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.node_type import NodeType
from bxcommon.services import sdn_http_service
from bxcommon.services.transaction_service import TransactionService
from bxgateway.services.block_recovery_service import BlockRecoveryService
from bxcommon.utils import logger
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway.connections.btc_node_connection import BTCNodeConnection
from bxgateway.connections.relay_connection import RelayConnection
from bxgateway.storage.block_encrypted_cache import BlockEncryptedCache
from bxgateway.testing.lossy_relay_connection import LossyRelayConnection
from bxgateway.testing.test_modes import TestModes
from bxgateway.testing.unencrypted_block_cache import UnencryptedCache


class GatewayNode(AbstractNode):
    node_type = NodeType.GATEWAY
    """
    bloXroute gateway node. Middlemans messages between blockchain nodes and the bloXroute
    relay network.

    Attributes
    ----------
    opts: node configuration options
    idx: node index, included in HelloMessages 
    node_params: blockchain node connection configuration dict
    node_conn: connection object to blockchain node
    in_progress_blocks: hash => (key, encrypted_block) dict of blocks for which keys
                        have not yet been released to the network
    block_recovery_service: service for finding unknown transaction short ids
    """

    def __init__(self, opts):
        super(GatewayNode, self).__init__(opts)

        self.opts = opts
        self.idx = constants.NULL_IDX

        self.tx_service = TransactionService(self)

        self.node_conn = None  # Connection object for the blockchain node
        self.node_msg_queue = deque()
        self.blocks_seen = ExpiringSet(self.alarm_queue, constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)

        if TestModes.DISABLE_ENCRYPTION in self.opts.test_mode:
            self.in_progress_blocks = UnencryptedCache(self.alarm_queue)
        else:
            self.in_progress_blocks = BlockEncryptedCache(self.alarm_queue)

        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)

    def send_request_for_peers(self):
        # Gateway uses rest, but relay uses messages.
        outbound_peers = sdn_http_service.fetch_outbound_peers(self.opts.node_id)

        self.on_updated_peers(outbound_peers)
        if not self.opts.outbound_peers:
            # Try again later.
            return constants.SDN_CONTACT_RETRY_SECONDS

    def get_outbound_peer_addresses(self):
        peers = []

        for peer in self.opts.outbound_peers:
            peers.append((peer.ip, peer.port))

        peers.append((self.opts.blockchain_ip, self.opts.blockchain_port))

        return peers

    def get_connection_class(self, ip=None, port=None):
        if self.opts.blockchain_ip == ip and self.opts.blockchain_port == port:
            return BTCNodeConnection
        else:
            return self._get_relay_connection_cls()

    def is_blockchain_node_address(self, ip, port):
        return ip == self.opts.blockchain_ip and port == self.opts.blockchain_port

    # Sends a message to the node that this is connected to
    def send_bytes_to_node(self, msg, prepend_to_queue=False):
        """
        Sends a message to the blockchain node this is connected to.
        """
        if self.node_conn is not None:
            logger.debug("Sending message to node: " + repr(msg))
            self.node_conn.enqueue_msg_bytes(msg, prepend_to_queue)
        else:
            logger.debug("Adding things to node's message queue")
            self.node_msg_queue.append(msg)

    def _get_relay_connection_cls(self):
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            return LossyRelayConnection
        else:
            return RelayConnection
