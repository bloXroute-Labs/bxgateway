from datetime import datetime
import time
from typing import Any
from typing import Optional, Iterable

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.utils import crypto, convert, logger
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import gateway_constants
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.services.block_recovery_service import BlockRecoveryInfo


class BlockHold(object):
    """
    Data class for holds on block messages.

    Attributes
    ----------
    hold_message_time: time the hold message was received. Currently unused, for use later to determine if Gateway is
                       sending malicious hold messages
    holding_connection: connection that sent the original hold message
    block_message: message being delayed due to an existing hold
    alarm: reference to alarm object for hold timeout
    held_connection: connection from which the message is being delayed for
    """

    def __init__(self, hold_message_time, holding_connection):
        self.hold_message_time = hold_message_time
        self.holding_connection = holding_connection

        self.block_message = None
        self.alarm = None
        self.held_connection = None


class BlockProcessingService(object):
    """
    Service class that process blocks.
    Blocks received from blockchain node are held if gateway receives a `blockhold` message from another gateway to
    prevent duplicate message sending.
    """

    def __init__(self, node):
        self._node = node
        self._holds = ExpiringDict(node.alarm_queue, gateway_constants.BLOCK_HOLD_DURATION_S)

    def place_hold(self, block_hash, connection):
        """
        Places hold on block hash and propagates message.
        :param block_hash: ObjectHash
        :param connection:
        """
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_REQUESTED,
                                                  network_num=connection.network_num,
                                                  peer=connection.peer_desc,
                                                  connection_type=connection.CONNECTION_TYPE)

        if block_hash in self._node.blocks_seen.contents:
            return

        if block_hash not in self._holds.contents:
            self._holds.add(block_hash, BlockHold(time.time(), connection))
            conns = self._node.broadcast(BlockHoldingMessage(block_hash, self._node.network_num),
                                         broadcasting_conn=connection,
                                         connection_types=[ConnectionType.RELAY, ConnectionType.GATEWAY])
            if len(conns) > 0:
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_HOLD_SENT_BY_GATEWAY_TO_PEERS,
                                                          network_num=self._node.network_num,
                                                          peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE),
                                                                    conns))

    def queue_block_for_processing(self, block_message, connection):
        """
        Queues up block for processing on timeout if hold message received.
        If no hold exists, compress and broadcast block immediately.
        :param block_message: block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        if block_hash in self._holds.contents:
            hold: BlockHold = self._holds.contents[block_hash]
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_HELD_BLOCK,
                                                      network_num=connection.network_num,
                                                      more_info=hold.holding_connection.peer_desc)

            if hold.alarm is None:
                hold.alarm = self._node.alarm_queue.register_alarm(gateway_constants.BLOCK_HOLDING_TIMEOUT_S,
                                                                   self._holding_timeout, block_hash, hold)
                hold.block_message = block_message
                hold.connection = connection
        else:
            # Broadcast BlockHoldingMessage through relays and gateways
            conns = self._node.broadcast(BlockHoldingMessage(block_hash, self._node.network_num),
                                         broadcasting_conn=connection,
                                         connection_types=[ConnectionType.RELAY, ConnectionType.GATEWAY],
                                         prepend_to_queue=True)
            if len(conns) > 0:
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_HOLD_SENT_BY_GATEWAY_TO_PEERS,
                                                          network_num=self._node.network_num,
                                                          peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE),
                                                                    conns))
            self._process_and_broadcast_block(block_message, connection)

    def cancel_hold_timeout(self, block_hash, connection):
        """
        Lifts hold on block hash and cancels timeout.
        :param block_hash: ObjectHash
        :param connection: connection cancelling hold
        """
        if block_hash in self._holds.contents:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_LIFTED,
                                                      network_num=connection.network_num,
                                                      peer=connection.peer_desc)

            hold = self._holds.contents[block_hash]
            if hold.alarm is not None:
                self._node.alarm_queue.unregister_alarm(hold.alarm)
            del self._holds.contents[block_hash]

    def process_block_broadcast(self, msg, connection):
        """
        Handle broadcast message receive from bloXroute.
        This is typically an encrypted block.
        """

        block_stats.add_block_event(msg,
                                    BlockStatEventType.ENC_BLOCK_RECEIVED_BY_GATEWAY_FROM_NETWORK,
                                    network_num=connection.network_num,
                                    peer=connection.peer_desc,
                                    connection_type=connection.CONNECTION_TYPE)

        block_hash = msg.block_hash()
        is_encrypted = msg.is_encrypted()

        if not is_encrypted:
            block = msg.blob()
            self._handle_decrypted_block(block, connection,
                                         encrypted_block_hash_hex=convert.bytes_to_hex(block_hash.binary))
            return

        cipherblob = msg.blob()
        if block_hash != Sha256Hash(crypto.double_sha256(cipherblob)):
            logger.warn("Received a message with inconsistent hashes. Dropping.")
            return

        if self._node.in_progress_blocks.has_encryption_key_for_hash(block_hash):
            logger.debug("Already had key for received block. Sending block to node.")
            decrypt_start = time.time()
            block = self._node.in_progress_blocks.decrypt_ciphertext(block_hash, cipherblob)

            if block is not None:
                block_stats.add_block_event(msg,
                                            BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS,
                                            network_num=connection.network_num,
                                            more_info="{:.2f}ms".format(time.time() - decrypt_start))
                self._handle_decrypted_block(block, connection,
                                             encrypted_block_hash_hex=convert.bytes_to_hex(block_hash.binary))
            else:
                block_stats.add_block_event(msg,
                                            BlockStatEventType.ENC_BLOCK_DECRYPTION_ERROR,
                                            network_num=connection.network_num)
        else:
            logger.debug("Received encrypted block. Storing.")
            self._node.in_progress_blocks.add_ciphertext(block_hash, cipherblob)
            block_received_message = BlockReceivedMessage(block_hash)
            conns = self._node.broadcast(block_received_message, self, connection_types=[ConnectionType.GATEWAY])
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.ENC_BLOCK_SENT_BLOCK_RECEIPT,
                                                      network_num=connection.network_num,
                                                      peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE),
                                                                conns))

    def process_block_key(self, msg, connection):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        key = msg.key()
        block_hash = msg.block_hash()

        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_RECEIVED_BY_GATEWAY_FROM_NETWORK,
                                                  network_num=connection.network_num,
                                                  peer=connection.peer_desc,
                                                  connection_type=connection.CONNECTION_TYPE)

        if self._node.in_progress_blocks.has_encryption_key_for_hash(block_hash):
            return

        if self._node.in_progress_blocks.has_ciphertext_for_hash(block_hash):
            logger.debug("Cipher text found. Decrypting and sending to node.")
            decrypt_start = time.time()
            block = self._node.in_progress_blocks.decrypt_and_get_payload(block_hash, key)

            if block is not None:
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS,
                                                          network_num=connection.network_num,
                                                          more_info="{:.2f}ms".format(time.time() - decrypt_start))
                self._handle_decrypted_block(block, connection,
                                             encrypted_block_hash_hex=convert.bytes_to_hex(block_hash.binary))
            else:
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.ENC_BLOCK_DECRYPTION_ERROR,
                                                          network_num=connection.network_num)
        else:
            logger.debug("No cipher text found on key message. Storing.")
            self._node.in_progress_blocks.add_key(block_hash, key)

        conns = self._node.broadcast(msg, connection, connection_types=[ConnectionType.GATEWAY])
        if len(conns) > 0:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.ENC_BLOCK_KEY_SENT_BY_GATEWAY_TO_PEERS,
                                                      network_num=self._node.network_num,
                                                      peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE),
                                                                conns))

    def retry_broadcast_recovered_blocks(self, connection):
        if self._node.block_recovery_service.recovered_blocks:
            for msg in self._node.block_recovery_service.recovered_blocks:
                self._handle_decrypted_block(msg, connection, recovered=True)

            self._node.block_recovery_service.clean_up_recovered_blocks()

    def _compute_hold_timeout(self, block_message):
        """
        Computes timeout after receiving block message before sending the block anyway if not received from network.
        TODO: implement algorithm for computing hold timeout
        :param block_message: block message to hold
        :return: time in seconds to wait
        """
        return gateway_constants.BLOCK_HOLDING_TIMEOUT_S

    def _holding_timeout(self, block_hash, hold):
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_TIMED_OUT,
                                                  network_num=hold.connection.network_num,
                                                  peer=hold.connection.peer_desc)
        self._process_and_broadcast_block(hold.block_message, hold.connection)

    def _process_and_broadcast_block(self, block_message, connection):
        """
        Compresses and propagates block message.
        :param block_message: block message to propagate
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        compress_start_datetime = datetime.utcnow()
        compress_start_timestamp = time.time()

        bx_block, block_info = connection.message_converter.block_to_bx_block(block_message,
                                                                              self._node.get_tx_service())
        original_size = len(block_message.rawbytes())
        compressed_size = len(bx_block)
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.BLOCK_COMPRESSED,
                                                  start_date_time=compress_start_datetime,
                                                  end_date_time=datetime.utcnow(),
                                                  network_num=connection.network_num,
                                                  original_size=original_size,
                                                  compressed_size=compressed_size,
                                                  compressed_hash=block_info.compressed_block_hash,
                                                  txs_count=block_info.txn_count,
                                                  prev_block_hash=block_info.prev_block_hash,
                                                  more_info="{:.2f}ms, {:.2f}%".format(
                                                      time.time() - compress_start_timestamp,
                                                      100 - float(compressed_size) / original_size * 100))
        self._process_and_broadcast_compressed_block(bx_block, connection, block_info, block_hash)

    def _process_and_broadcast_compressed_block(self, bx_block, connection, block_info, block_hash):
        """
        Process a compressed block.
        :param bx_block: compress block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        :param block_info: original block info
        :param block_hash: block hash
        """

        connection.node.neutrality_service.propagate_block_to_network(bx_block, connection, block_hash)

        connection.node.block_recovery_service.cancel_recovery_for_block(block_hash)
        connection.node.block_queuing_service.remove(block_hash)
        connection.node.blocks_seen.add(block_hash)
        self._node.get_tx_service().track_seen_short_ids(block_info.short_ids)

    def _handle_decrypted_block(self, bx_block, connection, encrypted_block_hash_hex=None, recovered=False):
        transaction_service = self._node.get_tx_service()
        decompress_start_datetime = datetime.utcnow()
        decompress_start_timestamp = time.time()
        # TODO: determine if a real block or test block. Discard if test block.
        block_message, block_hash, all_sids, unknown_sids, unknown_hashes = \
            self._node.node_conn.message_converter.bx_block_to_block(bx_block, transaction_service)

        decompress_end = datetime.utcnow()
        decompress_end_timestamp = time.time()

        self.cancel_hold_timeout(block_hash, connection)

        if recovered:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECOVERY_COMPLETED,
                                                      network_num=connection.network_num)

        if encrypted_block_hash_hex is None:
            encrypted_block_hash_hex = "Unknown"

        if block_hash in self._node.blocks_seen.contents:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_IGNORE_SEEN,
                                                      start_date_time=decompress_start_datetime,
                                                      end_date_time=decompress_end,
                                                      network_num=connection.network_num,
                                                      encrypted_block_hash=encrypted_block_hash_hex,
                                                      more_info="{:.2f}ms".format(
                                                          decompress_end_timestamp - decompress_start_timestamp))
            return

        if block_message is not None:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_SUCCESS,
                                                      start_date_time=decompress_start_datetime,
                                                      end_date_time=decompress_end,
                                                      network_num=connection.network_num,
                                                      encrypted_block_hash=encrypted_block_hash_hex,
                                                      more_info="{:.2f}ms".format(
                                                          decompress_end_timestamp - decompress_start_timestamp))
            if recovered or block_hash in self._node.block_queuing_service:
                self._node.block_queuing_service.update_recovered_block(block_hash, block_message)
            else:
                self._node.block_queuing_service.push(block_hash, block_message)

            self._node.block_recovery_service.cancel_recovery_for_block(block_hash)
            self._node.blocks_seen.add(block_hash)
            transaction_service.track_seen_short_ids(all_sids)
        else:
            if block_hash in self._node.block_queuing_service and not recovered:
                logger.debug("Handling already queued block again. Ignoring.")
                return

            self._node.block_recovery_service.add_block(bx_block, block_hash, unknown_sids, unknown_hashes)
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_DECOMPRESSED_WITH_UNKNOWN_TXS,
                                                      start_date_time=decompress_start_datetime,
                                                      end_date_time=decompress_end,
                                                      network_num=connection.network_num,
                                                      encrypted_block_hash=encrypted_block_hash_hex,
                                                      unknown_sids_count=len(unknown_sids),
                                                      unknown_hashes_count=len(unknown_hashes))

            self.start_transaction_recovery(unknown_sids, unknown_hashes, block_hash, connection)
            if recovered:
                # should never happen –– this should not be called on blocks that have not recovered
                logger.error("Unexpectedly, could not decompress block {} after block was recovered.", block_hash)
            else:
                self._node.block_queuing_service.push(block_hash, waiting_for_recovery=True)

    def start_transaction_recovery(self, unknown_sids: Iterable[int], unknown_hashes: Iterable[Sha256Hash],
                                   block_hash: Sha256Hash, connection: Optional[AbstractRelayConnection] = None):
        all_unknown_sids = []
        all_unknown_sids.extend(unknown_sids)
        tx_service = self._node.get_tx_service()

        # retrieving sids of txs with unknown contents
        for tx_hash in unknown_hashes:
            tx_sid = tx_service.get_short_id(tx_hash)
            all_unknown_sids.append(tx_sid)

        get_txs_message = GetTxsMessage(short_ids=all_unknown_sids)
        self._node.broadcast(get_txs_message, connection_types=[ConnectionType.RELAY])

        if connection is not None:
            tx_stats.add_txs_by_short_ids_event(
                all_unknown_sids,
                TransactionStatEventType.TX_UNKNOWN_SHORT_IDS_REQUESTED_BY_GATEWAY_FROM_RELAY,
                network_num=self._node.network_num,
                peer=connection.peer_desc,
                block_hash=convert.bytes_to_hex(block_hash.binary)
            )
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_STARTED,
                network_num=connection.network_num,
                request_hash=convert.bytes_to_hex(
                  crypto.double_sha256(get_txs_message.rawbytes()))
            )
        else:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_REPEATED,
                network_num=self._node.network_num,
                request_hash=convert.bytes_to_hex(
                  crypto.double_sha256(get_txs_message.rawbytes())
                )
            )

    def schedule_recovery_retry(self, block_awaiting_recovery: BlockRecoveryInfo):
        """
        Schedules a block recovery attempt. Repeated block recovery attempts result in longer timeouts,
        following `gateway_constants.BLOCK_RECOVERY_INTERVAL_S`'s pattern, until giving up.
        :param block_awaiting_recovery: info about recovering block
        :return:
        """
        block_hash = block_awaiting_recovery.block_hash
        recovery_attempts = self._node.block_recovery_service.recovery_attempts_by_block[block_hash]
        if recovery_attempts >= gateway_constants.BLOCK_RECOVERY_MAX_RETRY_ATTEMPTS:
            logger.error("Giving up on attempting to recover block: {}", block_hash)
        else:
            delay = gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[recovery_attempts]
            self._node.alarm_queue.register_approx_alarm(delay, delay / 2, self._trigger_recovery_retry,
                                                         block_awaiting_recovery)

    def _trigger_recovery_retry(self, block_awaiting_recovery: BlockRecoveryInfo):
        block_hash = block_awaiting_recovery.block_hash
        self._node.block_recovery_service.recovery_attempts_by_block[block_hash] += 1
        self.start_transaction_recovery(block_awaiting_recovery.unknown_short_ids,
                                        block_awaiting_recovery.unknown_transaction_hashes,
                                        block_hash)
