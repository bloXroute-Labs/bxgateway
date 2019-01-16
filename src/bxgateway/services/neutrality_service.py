from __future__ import division

import datetime

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.utils import logger, convert, crypto
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

    def register_for_block_receipts(self, cipher_hash, bx_block):
        """
        Register a block hash for receipts before broadcasting out key.
        :param cipher_hash: encrypted block ObjectHash
        :param bx_block compressed block
        """
        if cipher_hash in self._receipt_tracker:
            logger.warn("Ignoring duplicate bx_block hash for tracking receiving: {0}".format(cipher_hash))
            return

        self._receipt_tracker[cipher_hash] = 0
        if gateway_constants.NEUTRALITY_POLICY == NeutralityPolicy.RELEASE_IMMEDIATELY:
            logger.info("Neutrality policy: releasing key immediately.")
            self._send_key(cipher_hash)
        else:
            logger.info("Neutrality policy: waiting for receipts before releasing key.")
            alarm_id = self._node.alarm_queue.register_alarm(gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S,
                                                             lambda: self._propagate_block_to_gateway_peers(cipher_hash,
                                                                                                            bx_block))
            self._alarms[cipher_hash] = alarm_id

    def record_block_receipt(self, cipher_hash):
        """
        Records a receipt of a block hash. Releases key if threshold reached.
        :param cipher_hash encrypted block ObjectHash
        """
        if cipher_hash in self._receipt_tracker:
            self._receipt_tracker[cipher_hash] += 1
            block_stats.add_block_event_by_block_hash(cipher_hash,
                                                      BlockStatEventType.ENC_BLOCK_RECEIVED_BLOCK_RECEIPT,
                                                      network_num=self._node.network_num)

            if self._are_enough_receipts_received(cipher_hash):
                logger.info("Received enough block receipt messages. Releasing key for block with hash: {}"
                            .format(convert.bytes_to_hex(cipher_hash.binary)))
                self._send_key(cipher_hash)
                self._node.alarm_queue.unregister_alarm(self._alarms[cipher_hash])
                del self._receipt_tracker[cipher_hash]
                del self._alarms[cipher_hash]

    def propagate_block_to_network(self, bx_block, connection, block_hash=None):
        """
        Propagates encrypted block to bloXroute network and starts listening for block receipts.
        :param block_message: compressed block
        :param connection: connection initiating propagation
        :param block_hash: original block hash, only provided if this is the original block
        :return: broadcast message
        """
        if block_hash is None:
            block_hash = "Unknown"
            requested_by_peer = True
        else:
            requested_by_peer = False

        encrypt_start = datetime.datetime.utcnow()
        encrypted_block, raw_cipher_hash = self._node.in_progress_blocks.encrypt_and_add_payload(bx_block)
        encrypt_end = datetime.datetime.utcnow()
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.BLOCK_ENCRYPTED,
                                                  start_date_time=encrypt_start,
                                                  end_date_time=encrypt_end,
                                                  network_num=self._node.network_num,
                                                  encrypted_block_hash=convert.bytes_to_hex(raw_cipher_hash),
                                                  compressed_size=len(bx_block),
                                                  encrypted_size=len(encrypted_block))

        cipher_hash = ObjectHash(raw_cipher_hash)
        broadcast_message = BroadcastMessage(cipher_hash, self._node.network_num, encrypted_block)

        conns = self._node.broadcast(broadcast_message, connection)
        block_stats.add_block_event_by_block_hash(cipher_hash,
                                                  BlockStatEventType.ENC_BLOCK_SENT_FROM_GATEWAY_TO_NETWORK,
                                                  network_num=self._node.network_num,
                                                  requested_by_peer=requested_by_peer,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))
        self.register_for_block_receipts(cipher_hash, bx_block)
        return broadcast_message

    def _are_enough_receipts_received(self, cipher_hash):
        neutrality_policy = gateway_constants.NEUTRALITY_POLICY
        receipt_count = self._receipt_tracker[cipher_hash]

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

    def _propagate_block_to_gateway_peers(self, cipher_hash, bx_block):
        """
        Propagates unencrypted bx_block to all gateway peers for encryption and sending to bloXroute.
        Also sends keys to bloXroute in case this was user error (e.g. no gateway peers).
        Called after a timeout. This invalidates all future bx_block receipts.
        """
        bx_block_hash = crypto.double_sha256(bx_block)
        block_stats.add_block_event_by_block_hash(cipher_hash,
                                                  BlockStatEventType.ENC_BLOCK_PROPAGATION_NEEDED,
                                                  network_num=self._node.network_num,
                                                  compressed_block_hash=convert.bytes_to_hex(bx_block_hash))
        logger.warn("Did not receive bx_block receipts in timeout. Propagating bx_block to other gateways.")
        self._send_key(cipher_hash)

        request = BlockPropagationRequestMessage(bx_block)
        self._node.broadcast(request, None, connection_type=ConnectionType.GATEWAY)
        del self._receipt_tracker[cipher_hash]
        del self._alarms[cipher_hash]
        return constants.CANCEL_ALARMS

    def _send_key(self, cipher_hash):
        key = self._node.in_progress_blocks.get_encryption_key(bytes(cipher_hash.binary))
        key_message = KeyMessage(cipher_hash, key, self._node.network_num)
        conns = self._node.broadcast(key_message, None)
        block_stats.add_block_event_by_block_hash(cipher_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_SENT_FROM_GATEWAY_TO_PEER,
                                                  network_num=self._node.network_num,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))
