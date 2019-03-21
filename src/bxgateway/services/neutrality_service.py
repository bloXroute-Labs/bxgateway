from __future__ import division

import datetime
import time

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
            logger.debug("Neutrality policy: releasing key immediately.")
            self._send_key(cipher_hash)
        else:
            logger.debug("Neutrality policy: waiting for receipts before releasing key.")
            alarm_id = self._node.alarm_queue.register_alarm(gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S,
                                                             lambda: self._propagate_block_to_gateway_peers(cipher_hash,
                                                                                                            bx_block))
            self._alarms[cipher_hash] = alarm_id

    def record_block_receipt(self, cipher_hash, connection):
        """
        Records a receipt of a block hash. Releases key if threshold reached.
        :param cipher_hash encrypted block ObjectHash
        :param connection posting block received receipt
        """
        if cipher_hash in self._receipt_tracker:
            self._receipt_tracker[cipher_hash] += 1
            block_stats.add_block_event_by_block_hash(cipher_hash,
                                                      BlockStatEventType.ENC_BLOCK_RECEIVED_BLOCK_RECEIPT,
                                                      peer=(connection.peer_desc, connection.CONNECTION_TYPE),
                                                      receipt_count=self._receipt_tracker[cipher_hash],
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
        :param bx_block: compressed block
        :param connection: connection initiating propagation
        :param block_hash: original block hash, only provided if this is the original block
        :return: broadcast message
        """
        if self._node.opts.encrypt_blocks:
            broadcast_msg = self._propagate_encrypted_block_to_network(bx_block, connection, block_hash)
        else:
            broadcast_msg = self._propagate_unencrypted_block_to_network(bx_block, connection, block_hash)
        return broadcast_msg

    def _propagate_encrypted_block_to_network(self, bx_block, connection, block_hash):
        if block_hash is None:
            block_hash = b"Unknown"
            requested_by_peer = True
        else:
            requested_by_peer = False

        encrypt_start_datetime = datetime.datetime.utcnow()
        encrypt_start_timestamp = time.time()
        encrypted_block, raw_cipher_hash = self._node.in_progress_blocks.encrypt_and_add_payload(bx_block)

        compressed_size = len(bx_block)
        encrypted_size = len(encrypted_block)
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.BLOCK_ENCRYPTED,
                                                  start_date_time=encrypt_start_datetime,
                                                  end_date_time=datetime.datetime.utcnow(),
                                                  network_num=self._node.network_num,
                                                  encrypted_block_hash=convert.bytes_to_hex(raw_cipher_hash),
                                                  compressed_size=compressed_size,
                                                  encrypted_size=encrypted_size,
                                                  more_info="{:.2f}ms; {:.2f}%".format(
                                                      time.time() - encrypt_start_timestamp,
                                                      100 - float(encrypted_size) / compressed_size * 100
                                                  ))

        cipher_hash = ObjectHash(raw_cipher_hash)
        broadcast_message = BroadcastMessage(cipher_hash, self._node.network_num, True, encrypted_block)

        conns = self._node.broadcast(broadcast_message, connection)
        block_stats.add_block_event_by_block_hash(cipher_hash,
                                                  BlockStatEventType.ENC_BLOCK_SENT_FROM_GATEWAY_TO_NETWORK,
                                                  network_num=self._node.network_num,
                                                  requested_by_peer=requested_by_peer,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns),
                                                  more_info="encrypted")
        self.register_for_block_receipts(cipher_hash, bx_block)
        return broadcast_message

    def _propagate_unencrypted_block_to_network(self, bx_block, connection, block_hash):
        if block_hash is None:
            raise ValueError("Block hash is required to propagate unencrypted block")

        broadcast_message = BroadcastMessage(block_hash, self._node.network_num, False, bx_block)
        conns = self._node.broadcast(broadcast_message, connection)
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.ENC_BLOCK_SENT_FROM_GATEWAY_TO_NETWORK,
                                                  network_num=self._node.network_num,
                                                  requested_by_peer=False,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns),
                                                  more_info="unencrypted")
        return broadcast_message

    def _are_enough_receipts_received(self, cipher_hash):
        neutrality_policy = gateway_constants.NEUTRALITY_POLICY
        receipt_count = self._receipt_tracker[cipher_hash]

        enough_by_count = receipt_count >= gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_COUNT

        active_gateway_peer_count = len(
            list(filter(lambda conn: conn.is_active(),
                   self._node.connection_pool.get_by_connection_type(ConnectionType.GATEWAY))))
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
        hex_bx_block_hash = convert.bytes_to_hex(bx_block_hash)

        logger.warn("Did not receive enough receipts for: {}. Propagating compressed block to other gateways: {}"
                    .format(cipher_hash, hex_bx_block_hash))
        self._send_key(cipher_hash)

        request = BlockPropagationRequestMessage(bx_block)
        conns = self._node.broadcast(request, None, connection_types=[ConnectionType.GATEWAY])
        block_stats.add_block_event_by_block_hash(cipher_hash,
                                                  BlockStatEventType.ENC_BLOCK_PROPAGATION_NEEDED,
                                                  network_num=self._node.network_num,
                                                  compressed_block_hash=hex_bx_block_hash,
                                                  receipt_count=self._receipt_tracker[cipher_hash],
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))

        del self._receipt_tracker[cipher_hash]
        del self._alarms[cipher_hash]
        return constants.CANCEL_ALARMS

    def _send_key(self, cipher_hash):
        key = self._node.in_progress_blocks.get_encryption_key(bytes(cipher_hash.binary))
        key_message = KeyMessage(cipher_hash, self._node.network_num, key)
        conns = self._node.broadcast(key_message, None, connection_types=[ConnectionType.RELAY, ConnectionType.GATEWAY])
        block_stats.add_block_event_by_block_hash(cipher_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_SENT_FROM_GATEWAY_TO_NETWORK,
                                                  network_num=self._node.network_num,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))
