from collections import deque

from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.connections.node_types import NodeTypes
from bxcommon.constants import NULL_IDX
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.services.block_recovery_service import BlockRecoveryService
from bxcommon.utils import logger
from bxgateway.connections.btc_node_connection import BTCNodeConnection
from bxgateway.connections.relay_connection import RelayConnection
from bxgateway.testing.lossy_relay_connection import LossyRelayConnection
from bxgateway.testing.test_modes import TestModes


class GatewayNode(AbstractNode):
    node_type = NodeTypes.GATEWAY

    def __init__(self, opts):
        super(GatewayNode, self).__init__(opts)

        self.opts = opts
        self.idx = NULL_IDX

        self.node_conn = None  # Connection object for the blockchain node
        self.node_msg_queue = deque()

        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)

    def get_outbound_peer_addresses(self):
        peers = []

        for peer in self.opts.outbound_peers:
            peers.append((peer.get(OutboundPeerModel.ip), peer.get(OutboundPeerModel.port)))

        peers.append((self.opts.blockchain_ip, self.opts.blockchain_port))

        return peers

    def get_connection_class(self, ip=None, port=None):
        return BTCNodeConnection \
            if self.opts.blockchain_ip == ip and self.opts.blockchain_port == port \
            else self._get_relay_connection_cls()

    def is_blockchain_node_address(self, ip, port):
        return ip == self.opts.blockchain_ip and port == self.opts.blockchain_port

    # Sends a message to the node that this is connected to
    def send_bytes_to_node(self, msg):
        if self.node_conn is not None:
            logger.debug("Sending message to node: " + repr(msg))
            self.node_conn.enqueue_msg_bytes(msg)
        else:
            logger.debug("Adding things to node's message queue")
            self.node_msg_queue.append(msg)

    def _get_relay_connection_cls(self):
        logger.debug("Get Relay connection")

        if self.opts.test_mode == TestModes.DROPPING_TXS_MODE:
            return LossyRelayConnection
        else:
            return RelayConnection
