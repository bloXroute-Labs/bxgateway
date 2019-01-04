from __future__ import division

import datetime

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.utils import logger, convert
from bxcommon.utils.object_hash import ObjectHash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxgateway.gateway_constants import NeutralityPolicy
from bxgateway.messages.gateway.block_propagation_request import BlockPropagationRequestMessage


class NeutralityService(object):
    """
    Service to manage block encryption and ensure network is neutral to Gateway Node's requests.
    """

    def __init__(self, node):
        self._node = node
        self._receipt_tracker = {}
        self._alarms = {}

    def register_for_block_receipts(self, block_hash, block):
        """
        Register a block hash for receipts before broadcasting out key.
        :param block_hash ObjectHash
        :param block bloXroute block
        """
        if block_hash in self._receipt_tracker:
            logger.warn("Ignoring duplicate block hash for tracking receiving: {0}".format(block_hash))
            return

        self._receipt_tracker[block_hash] = 0
        if gateway_constants.NEUTRALITY_POLICY == NeutralityPolicy.RELEASE_IMMEDIATELY:
            logger.info("Neutrality policy: releasing key immediately.")
            self._send_key(block_hash)
        else:
            logger.info("Neutrality policy: waiting for receipts before releasing key.")
            alarm_id = self._node.alarm_queue.register_alarm(gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S,
                                                             lambda: self._propagate_block_to_gateway_peers(block_hash,
                                                                                                            block))
            self._alarms[block_hash] = alarm_id

    def record_block_receipt(self, block_hash):
        """
        Records a receipt of a block hash. Releases key if threshold reached.
        :param block_hash ObjectHash
        """
        if block_hash in self._receipt_tracker:
            self._receipt_tracker[block_hash] += 1
            if self._are_enough_receipts_received(block_hash):
                logger.info("Received enough block receipt messages. Releasing key for block with hash: {}"
                            .format(convert.bytes_to_hex(block_hash.binary)))
                self._send_key(block_hash)
                self._node.alarm_queue.unregister_alarm(self._alarms[block_hash])
                del self._receipt_tracker[block_hash]
                del self._alarms[block_hash]

    def propagate_block_to_network(self, block_message, connection):
        """
        Propagates encrypted block to bloXroute network and starts listening for block receipts.
        :param block_message: blockchain message
        :param connection: connection initiating propagation
        :return: broadcast message
        """
        compress_start = datetime.datetime.utcnow()

        bx_block = connection.message_converter.block_to_bx_block(block_message, connection.node.get_tx_service())
        encrypted_block, raw_bx_block_hash = self._node.in_progress_blocks.encrypt_and_add_payload(bx_block)
        bx_block_hash = ObjectHash(raw_bx_block_hash)

        compress_end = datetime.datetime.utcnow()
        block_stats.add_block_event_by_block_hash(block_message.block_hash(),
                                                  BlockStatEventType.BLOCK_COMPRESSED,
                                                  start_date_time=compress_start,
                                                  end_date_time=compress_end,
                                                  encrypted_block_hash=convert.bytes_to_hex(raw_bx_block_hash),
                                                  original_size=len(block_message.rawbytes()),
                                                  compressed_size=len(bx_block))

        broadcast_message = BroadcastMessage(bx_block_hash, self._node.network_num, encrypted_block)

        conns = self._node.broadcast(broadcast_message, connection)
        block_stats.add_block_event_by_block_hash(bx_block_hash,
                                                  BlockStatEventType.ENC_BLOCK_SENT_FROM_GATEWAY_TO_PEER,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))
        self.register_for_block_receipts(bx_block_hash, bx_block)
        return broadcast_message

    def _are_enough_receipts_received(self, block_hash):
        neutrality_policy = gateway_constants.NEUTRALITY_POLICY
        receipt_count = self._receipt_tracker[block_hash]

        enough_by_count = receipt_count >= gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_COUNT

        active_gateway_peer_count = len(
            filter(lambda conn: conn.is_active(),
                   self._node.connection_pool.get_by_connection_type(ConnectionType.GATEWAY)))
        if active_gateway_peer_count == 0:
            logger.warn("No active gateway peers to get block receipts from.")
            enough_by_percent = False
        else:
            enough_by_percent = (receipt_count / active_gateway_peer_count * 100 >=
                                 gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_PERCENT)

        if neutrality_policy == NeutralityPolicy.RECEIPT_COUNT:
            return enough_by_count
        elif neutrality_policy == NeutralityPolicy.RECEIPT_PERCENT:
            return enough_by_percent
        elif neutrality_policy == NeutralityPolicy.RECEIPT_COUNT_AND_PERCENT:
            return enough_by_count and enough_by_percent

        raise ValueError("Unexpected neutrality policy: {}".format(neutrality_policy))

    def _propagate_block_to_gateway_peers(self, block_hash, block):
        """
        Propagates block to all gateway peers for encryption and sending to bloXroute.
        Also sends keys to bloXroute in case this was user error (e.g. no gateway peers).
        Called after a timeout. This invalidates all future block receipts.
        """
        logger.warn("Did not receive block receipts in timeout. Propagating block to other gateways.")
        self._send_key(block_hash)

        request = BlockPropagationRequestMessage(block)
        self._node.broadcast(request, None, connection_type=ConnectionType.GATEWAY)
        del self._receipt_tracker[block_hash]
        del self._alarms[block_hash]
        return constants.CANCEL_ALARMS

    def _send_key(self, block_hash):
        key = self._node.in_progress_blocks.get_encryption_key(bytes(block_hash.binary))
        key_message = KeyMessage(block_hash, key, self._node.network_num)
        conns = self._node.broadcast(key_message, None)
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_SENT_FROM_GATEWAY_TO_PEER,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))
