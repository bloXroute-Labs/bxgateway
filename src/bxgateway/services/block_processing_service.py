import datetime
import time
from typing import Iterable, Optional, TYPE_CHECKING, Union

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.feed.feed_source import FeedSource
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.messages.eth.validation.abstract_block_validator import AbstractBlockValidator, \
    BlockValidationResult
from bxcommon.utils import convert, crypto, block_content_debug_utils
from bxcommon.utils.blockchain_utils.eth import eth_common_utils
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.limited_size_set import LimitedSizeSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.stat_block_type import StatBlockType
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import gateway_constants
from bxgateway import log_messages
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.services.block_recovery_service import BlockRecoveryInfo, RecoveredTxsSource
from bxgateway.utils.errors.message_conversion_error import MessageConversionError
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class BlockHold:
    """
    Data class for holds on block messages.

    Attributes
    ----------
    hold_message_time: time the hold message was received. Currently unused, for use later to determine if Gateway is
                       sending malicious hold messages
    holding_connection: connection that sent the original hold message
    block_message: message being delayed due to an existing hold
    alarm: reference to alarm object for hold timeout
    connection: connection from which the message is being delayed for
    """

    def __init__(self, hold_message_time, holding_connection):
        self.hold_message_time = hold_message_time
        self.holding_connection = holding_connection

        self.block_message = None
        self.alarm = None
        self.connection = None


class BlockProcessingService:
    """
    Service class that process blocks.
    Blocks received from blockchain node are held if gateway receives a `blockhold` message from another gateway to
    prevent duplicate message sending.
    """

    def __init__(self, node):
        self._node: AbstractGatewayNode = node
        self._holds = ExpiringDict(
            node.alarm_queue,
            node.opts.blockchain_block_hold_timeout_s,
            f"block_processing_holds"
        )

        self._block_validator: Optional[AbstractBlockValidator] = None
        self._last_confirmed_block_number: Optional[int] = None
        self._last_confirmed_block_difficulty: Optional[int] = None
        self._blocks_failed_validation_history: LimitedSizeSet[Sha256Hash] = LimitedSizeSet(
            constants.BLOCKS_FAILED_VALIDATION_HISTORY_SIZE)

    def place_hold(self, block_hash, connection) -> None:
        """
        Places hold on block hash and propagates message.
        :param block_hash: ObjectHash
        :param connection:
        """
        block_stats.add_block_event_by_block_hash(
            block_hash, BlockStatEventType.BLOCK_HOLD_REQUESTED,
            network_num=connection.network_num,
            peers=[connection],
        )

        if block_hash in self._node.blocks_seen.contents:
            return

        if block_hash not in self._holds.contents:
            self._holds.add(block_hash, BlockHold(time.time(), connection))
            conns = self._node.broadcast(
                BlockHoldingMessage(block_hash, self._node.network_num),
                broadcasting_conn=connection,
                connection_types=(ConnectionType.RELAY_BLOCK, ConnectionType.GATEWAY)
            )
            if len(conns) > 0:
                block_stats.add_block_event_by_block_hash(
                    block_hash,
                    BlockStatEventType.BLOCK_HOLD_SENT_BY_GATEWAY_TO_PEERS,
                    network_num=self._node.network_num,
                    peers=conns,
                )

    def queue_block_for_processing(self, block_message, connection) -> None:
        """
        Queues up block for processing on timeout if hold message received.
        If no hold exists, compress and broadcast block immediately.
        :param block_message: block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        """

        block_hash = block_message.block_hash()
        connection.log_info(
            "Processing block {} from local blockchain node.",
            block_hash
        )

        valid_block = self._validate_block_header_in_block_message(block_message)
        if not valid_block.is_valid:
            reason = valid_block.reason
            assert reason is not None
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_FAILED_VALIDATION,
                connection.network_num,
                more_info=reason
            )
            return

        if block_hash in self._holds.contents:
            hold: BlockHold = self._holds.contents[block_hash]
            block_stats.add_block_event_by_block_hash(
                block_hash, BlockStatEventType.BLOCK_HOLD_HELD_BLOCK,
                network_num=connection.network_num,
                peers=[hold.holding_connection],
            )
            if hold.alarm is None:
                hold.alarm = self._node.alarm_queue.register_alarm(
                    self._node.opts.blockchain_block_hold_timeout_s, self._holding_timeout, block_hash, hold
                )
                hold.block_message = block_message
                hold.connection = connection
        else:
            if self._node.opts.encrypt_blocks:
                # Broadcast holding message if gateway wants to encrypt blocks
                conns = self._node.broadcast(
                    BlockHoldingMessage(block_hash, self._node.network_num),
                    broadcasting_conn=connection,
                    prepend_to_queue=True,
                    connection_types=(ConnectionType.RELAY_BLOCK, ConnectionType.GATEWAY)
                )
                if len(conns) > 0:
                    block_stats.add_block_event_by_block_hash(
                        block_hash,
                        BlockStatEventType.BLOCK_HOLD_SENT_BY_GATEWAY_TO_PEERS,
                        network_num=self._node.network_num,
                        peers=conns
                    )
            self._process_and_broadcast_block(block_message, connection)

    def cancel_hold_timeout(self, block_hash, connection) -> None:
        """
        Lifts hold on block hash and cancels timeout.
        :param block_hash: ObjectHash
        :param connection: connection cancelling hold
        """
        if block_hash in self._holds.contents:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_LIFTED,
                                                      network_num=connection.network_num,
                                                      peers=[connection],
                                                      )

            hold = self._holds.contents[block_hash]
            if hold.alarm is not None:
                self._node.alarm_queue.unregister_alarm(hold.alarm)
            del self._holds.contents[block_hash]

    def process_block_broadcast(self, msg, connection: AbstractRelayConnection) -> None:
        """
        Handle broadcast message receive from bloXroute.
        This is typically an encrypted block.
        """

        # TODO handle the situation where txs that received from relays while syncing are in the blocks that were
        #  ignored while syncing, so these txs won't be cleaned for 3 days
        if not self._node.should_process_block_hash(msg.block_hash()):
            return

        block_stats.add_block_event(
            msg,
            BlockStatEventType.ENC_BLOCK_RECEIVED_BY_GATEWAY_FROM_NETWORK,
            network_num=connection.network_num,
            more_info=stats_format.connection(connection)
        )

        block_hash = msg.block_hash()
        is_encrypted = msg.is_encrypted()
        self._node.track_block_from_bdn_handling_started(block_hash, connection.peer_desc)

        if not is_encrypted:
            block = msg.blob()
            self._handle_decrypted_block(block, connection)
            return

        cipherblob = msg.blob()
        expected_hash = Sha256Hash(crypto.double_sha256(cipherblob))
        if block_hash != expected_hash:
            connection.log_warning(log_messages.BLOCK_WITH_INCONSISTENT_HASHES,
                                   expected_hash, block_hash)
            return

        if self._node.in_progress_blocks.has_encryption_key_for_hash(block_hash):
            connection.log_trace("Already had key for received block. Sending block to node.")
            decrypt_start_timestamp = time.time()
            decrypt_start_datetime = datetime.datetime.utcnow()
            block = self._node.in_progress_blocks.decrypt_ciphertext(block_hash, cipherblob)

            if block is not None:
                block_stats.add_block_event(
                    msg,
                    BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS,
                    start_date_time=decrypt_start_datetime,
                    end_date_time=datetime.datetime.utcnow(),
                    network_num=connection.network_num,
                    more_info=stats_format.timespan(decrypt_start_timestamp, time.time())
                )
                self._handle_decrypted_block(
                    block,
                    connection,
                    encrypted_block_hash_hex=convert.bytes_to_hex(block_hash.binary)
                )
            else:
                block_stats.add_block_event(msg,
                                            BlockStatEventType.ENC_BLOCK_DECRYPTION_ERROR,
                                            network_num=connection.network_num)
        else:
            connection.log_trace("Received encrypted block. Storing.")
            self._node.in_progress_blocks.add_ciphertext(block_hash, cipherblob)
            block_received_message = BlockReceivedMessage(block_hash)
            conns = self._node.broadcast(
                block_received_message,
                connection,
                connection_types=(ConnectionType.GATEWAY,)
            )
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.ENC_BLOCK_SENT_BLOCK_RECEIPT,
                network_num=connection.network_num,
                peers=conns,
            )

    def process_block_key(self, msg, connection: AbstractRelayConnection) -> None:
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        key = msg.key()
        block_hash = msg.block_hash()

        if not self._node.should_process_block_hash(block_hash):
            return

        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_RECEIVED_BY_GATEWAY_FROM_NETWORK,
                                                  network_num=connection.network_num,
                                                  peers=[connection])

        if self._node.in_progress_blocks.has_encryption_key_for_hash(block_hash):
            return

        if self._node.in_progress_blocks.has_ciphertext_for_hash(block_hash):
            connection.log_trace("Cipher text found. Decrypting and sending to node.")
            decrypt_start_timestamp = time.time()
            decrypt_start_datetime = datetime.datetime.utcnow()
            block = self._node.in_progress_blocks.decrypt_and_get_payload(block_hash, key)

            if block is not None:
                block_stats.add_block_event_by_block_hash(
                    block_hash,
                    BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS,
                    start_date_time=decrypt_start_datetime,
                    end_date_time=datetime.datetime.utcnow(),
                    network_num=connection.network_num,
                    more_info=stats_format.timespan(
                        decrypt_start_timestamp,
                        time.time())
                )
                self._handle_decrypted_block(
                    block, connection,
                    encrypted_block_hash_hex=convert.bytes_to_hex(block_hash.binary)
                )
            else:
                block_stats.add_block_event_by_block_hash(
                    block_hash,
                    BlockStatEventType.ENC_BLOCK_DECRYPTION_ERROR,
                    network_num=connection.network_num
                )
        else:
            connection.log_trace("No cipher text found on key message. Storing.")
            self._node.in_progress_blocks.add_key(block_hash, key)

        conns = self._node.broadcast(msg, connection, connection_types=(ConnectionType.GATEWAY,))
        if len(conns) > 0:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.ENC_BLOCK_KEY_SENT_BY_GATEWAY_TO_PEERS,
                                                      network_num=self._node.network_num,
                                                      peers=conns
                                                      )

    def retry_broadcast_recovered_blocks(self, connection) -> None:
        if self._node.block_recovery_service.recovered_blocks and self._node.opts.has_fully_updated_tx_service:
            for msg, recovery_source in self._node.block_recovery_service.recovered_blocks:
                self._handle_decrypted_block(msg, connection, recovered=True, recovered_txs_source=recovery_source)

            self._node.block_recovery_service.clean_up_recovered_blocks()

    def reset_last_confirmed_block_parameters(self):
        self._last_confirmed_block_number = None
        self._last_confirmed_block_difficulty = None

    def set_last_confirmed_block_parameters(
        self,
        last_confirmed_block_number: int,
        last_confirmed_block_difficulty: int
    ) -> None:

        if not self._node.peer_relays:
            logger.debug("Skip updating last confirmed block parameters because there is no connection to block relay")
            return

        if self._last_confirmed_block_number is not None:

            old_last_confirmed_block_number = self._last_confirmed_block_number
            assert old_last_confirmed_block_number is not None

            if last_confirmed_block_number < old_last_confirmed_block_number:
                logger.trace("New last confirmed block number {} is smaller than current {}. Skipping operation.",
                             last_confirmed_block_number, old_last_confirmed_block_number)
                return

        self._last_confirmed_block_number = last_confirmed_block_number
        self._last_confirmed_block_difficulty = last_confirmed_block_difficulty

        logger.trace("Updated last confirmed block number to {} and difficulty to {}.",
                     self._last_confirmed_block_number, self._last_confirmed_block_difficulty)

    def _compute_hold_timeout(self, _block_message) -> int:
        """
        Computes timeout after receiving block message before sending the block anyway if not received from network.
        TODO: implement algorithm for computing hold timeout
        :param block_message: block message to hold
        :return: time in seconds to wait
        """
        return self._node.opts.blockchain_block_hold_timeout_s

    def _holding_timeout(self, block_hash, hold):
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_TIMED_OUT,
                                                  network_num=hold.connection.network_num,
                                                  peers=[hold.connection]
                                                  )
        self._process_and_broadcast_block(hold.block_message, hold.connection)

    def _process_and_broadcast_block(self, block_message, connection: AbstractGatewayBlockchainConnection) -> None:
        """
        Compresses and propagates block message if enabled, else return.
        :param block_message: block message to propagate
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        message_converter = self._node.message_converter
        assert message_converter is not None
        try:
            bx_block, block_info = message_converter.block_to_bx_block(
                block_message,
                self._node.get_tx_service(),
                self._node.opts.enable_block_compression,
                self._node.network.min_tx_age_seconds
            )
        except MessageConversionError as e:
            block_stats.add_block_event_by_block_hash(
                e.msg_hash,
                BlockStatEventType.BLOCK_CONVERSION_FAILED,
                network_num=connection.network_num,
                conversion_type=e.conversion_type.value
            )
            connection.log_error(log_messages.BLOCK_COMPRESSION_FAIL, e.msg_hash, e)
            return

        if block_info.ignored_short_ids:
            assert block_info.ignored_short_ids is not None
            logger.debug(
                "Ignoring {} new SIDs for {}: {}",
                len(block_info.ignored_short_ids), block_info.block_hash, block_info.ignored_short_ids
            )

        compression_rate = block_info.compression_rate
        assert compression_rate is not None
        block_stats.add_block_event_by_block_hash(
            block_hash,
            BlockStatEventType.BLOCK_COMPRESSED,
            start_date_time=block_info.start_datetime,
            end_date_time=block_info.end_datetime,
            network_num=connection.network_num,
            prev_block_hash=block_info.prev_block_hash,
            original_size=block_info.original_size,
            txs_count=block_info.txn_count,
            blockchain_network=self._node.opts.blockchain_network,
            blockchain_protocol=self._node.opts.blockchain_protocol,
            matching_block_hash=block_info.compressed_block_hash,
            matching_block_type=StatBlockType.COMPRESSED.value,
            more_info="Compression: {}->{} bytes, {}, {}; Tx count: {}".format(
                block_info.original_size,
                block_info.compressed_size,
                stats_format.percentage(compression_rate),
                stats_format.duration(block_info.duration_ms),
                block_info.txn_count
            )
        )
        if self._node.opts.dump_short_id_mapping_compression:
            mapping = {}
            for short_id in block_info.short_ids:
                tx_hash = self._node.get_tx_service().get_transaction(short_id).hash
                assert tx_hash is not None
                mapping[short_id] = convert.bytes_to_hex(tx_hash.binary)
            with open(f"{self._node.opts.dump_short_id_mapping_compression_path}/"
                      f"{convert.bytes_to_hex(block_hash.binary)}", "w") as f:
                f.write(str(mapping))

        self._process_and_broadcast_compressed_block(bx_block, connection, block_info, block_hash)

        self._node.log_blocks_network_content(self._node.network_num, block_message)

    def _process_and_broadcast_compressed_block(
        self,
        bx_block,
        connection: AbstractGatewayBlockchainConnection,
        block_info,
        block_hash: Sha256Hash
    ) -> None:
        """
        Process a compressed block.
        :param bx_block: compress block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        :param block_info: original block info
        :param block_hash: block hash
        """
        connection.node.neutrality_service.propagate_block_to_network(bx_block, connection, block_info)
        self._node.get_tx_service().track_seen_short_ids_delayed(block_hash, block_info.short_ids)

    def _handle_decrypted_block(
        self,
        bx_block: memoryview,
        connection: AbstractRelayConnection,
        encrypted_block_hash_hex: Optional[str] = None,
        recovered: bool = False,
        recovered_txs_source: Optional[RecoveredTxsSource] = None
    ) -> None:
        transaction_service = self._node.get_tx_service()
        message_converter = self._node.message_converter
        assert message_converter is not None

        valid_block = self._validate_compressed_block_header(bx_block)
        if not valid_block.is_valid:
            reason = valid_block.reason
            assert reason is not None
            block_stats.add_block_event_by_block_hash(
                valid_block.block_hash,
                BlockStatEventType.BLOCK_DECOMPRESSED_FAILED_VALIDATION,
                connection.network_num,
                more_info=reason
            )
            return

        # TODO: determine if a real block or test block. Discard if test block.
        if self._node.remote_node_conn or self._node.has_active_blockchain_peer():
            try:
                (
                    block_message, block_info, unknown_sids, unknown_hashes
                ) = message_converter.bx_block_to_block(bx_block, transaction_service)
                block_content_debug_utils.log_compressed_block_debug_info(transaction_service, bx_block)
            except MessageConversionError as e:
                block_stats.add_block_event_by_block_hash(
                    e.msg_hash,
                    BlockStatEventType.BLOCK_CONVERSION_FAILED,
                    network_num=connection.network_num,
                    conversion_type=e.conversion_type.value
                )
                transaction_service.on_block_cleaned_up(e.msg_hash)
                connection.log_warning(log_messages.FAILED_TO_DECOMPRESS_BLOCK, e.msg_hash, e)
                return
        else:
            connection.log_warning(log_messages.LACK_BLOCKCHAIN_CONNECTION)
            return

        block_hash = block_info.block_hash
        all_sids = block_info.short_ids

        if encrypted_block_hash_hex is not None:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_TO_ENC_BLOCK_MATCH,
                matching_block_hash=encrypted_block_hash_hex,
                matching_block_type=StatBlockType.ENCRYPTED.value,
                network_num=connection.network_num
            )

        self.cancel_hold_timeout(block_hash, connection)

        if recovered:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_COMPLETED,
                network_num=connection.network_num,
                more_info=str(recovered_txs_source)
            )

        if block_hash in self._node.blocks_seen.contents:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_DECOMPRESSED_IGNORE_SEEN,
                start_date_time=block_info.start_datetime,
                end_date_time=block_info.end_datetime,
                network_num=connection.network_num,
                prev_block_hash=block_info.prev_block_hash,
                original_size=block_info.original_size,
                compressed_size=block_info.compressed_size,
                txs_count=block_info.txn_count,
                blockchain_network=self._node.opts.blockchain_network,
                blockchain_protocol=self._node.opts.blockchain_protocol,
                matching_block_hash=block_info.compressed_block_hash,
                matching_block_type=StatBlockType.COMPRESSED.value,
                more_info=stats_format.duration(block_info.duration_ms)
            )
            self._node.track_block_from_bdn_handling_ended(block_hash)
            transaction_service.track_seen_short_ids(block_hash, all_sids)
            connection.log_info(
                "Discarding duplicate block {} from the BDN.",
                block_hash
            )
            if block_message is not None:
                self._node.on_block_received_from_bdn(block_hash, block_message)
                if self._node.block_queuing_service_manager.get_block_data(block_hash) is None:
                    self._node.block_queuing_service_manager.store_block_data(block_hash, block_message)
            return

        if not recovered:
            connection.log_info("Received block {} from the BDN.", block_hash)
        else:
            connection.log_info("Successfully recovered block {}.", block_hash)

        if block_message is not None:
            compression_rate = block_info.compression_rate
            assert compression_rate is not None
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_DECOMPRESSED_SUCCESS,
                start_date_time=block_info.start_datetime,
                end_date_time=block_info.end_datetime,
                network_num=connection.network_num,
                prev_block_hash=block_info.prev_block_hash,
                original_size=block_info.original_size,
                compressed_size=block_info.compressed_size,
                txs_count=block_info.txn_count,
                blockchain_network=self._node.opts.blockchain_network,
                blockchain_protocol=self._node.opts.blockchain_protocol,
                matching_block_hash=block_info.compressed_block_hash,
                matching_block_type=StatBlockType.COMPRESSED.value,
                more_info="Compression rate {}, Decompression time {}, "
                          "Queued behind {} blocks".format(
                    stats_format.percentage(compression_rate),
                    stats_format.duration(block_info.duration_ms),
                    self._node.block_queuing_service_manager.get_length_of_each_queuing_service_stats_format()
                )
            )

            self._on_block_decompressed(block_message)
            if recovered or self._node.block_queuing_service_manager.is_in_any_queuing_service(block_hash):
                self._node.block_queuing_service_manager.update_recovered_block(block_hash, block_message)
            else:
                self._node.block_queuing_service_manager.push(block_hash, block_message)

            gateway_bdn_performance_stats_service.log_block_from_bdn()

            self._node.on_block_received_from_bdn(block_hash, block_message)
            transaction_service.track_seen_short_ids(block_hash, all_sids)

            self._node.publish_block(None, block_hash, block_message, FeedSource.BDN_SOCKET)
            self._node.log_blocks_network_content(self._node.network_num, block_message)
        else:
            if self._node.block_queuing_service_manager.is_in_any_queuing_service(block_hash) and not recovered:
                connection.log_trace("Handling already queued block again. Ignoring.")
                return

            self._node.block_recovery_service.add_block(bx_block, block_hash, unknown_sids, unknown_hashes)
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_DECOMPRESSED_WITH_UNKNOWN_TXS,
                start_date_time=block_info.start_datetime,
                end_date_time=block_info.end_datetime,
                network_num=connection.network_num,
                prev_block_hash=block_info.prev_block_hash,
                original_size=block_info.original_size,
                compressed_size=block_info.compressed_size,
                txs_count=block_info.txn_count,
                blockchain_network=self._node.opts.blockchain_network,
                blockchain_protocol=self._node.opts.blockchain_protocol,
                matching_block_hash=block_info.compressed_block_hash,
                matching_block_type=StatBlockType.COMPRESSED.value,
                more_info="{} sids, {} hashes, [{},...]".format(len(unknown_sids), len(unknown_hashes),
                                                                unknown_sids[:5])
            )

            connection.log_info("Block {} requires short id recovery. Querying BDN...", block_hash)

            self.start_transaction_recovery(unknown_sids, unknown_hashes, block_hash, connection)
            if recovered:
                # should never happen –– this should not be called on blocks that have not recovered
                connection.log_error(log_messages.BLOCK_DECOMPRESSION_FAILURE,
                                     block_hash)
            else:
                self._node.block_queuing_service_manager.push(
                    block_hash, waiting_for_recovery=True
                )

    def start_transaction_recovery(
        self,
        unknown_sids: Iterable[int],
        unknown_hashes: Iterable[Sha256Hash],
        block_hash: Sha256Hash,
        connection: Optional[AbstractRelayConnection] = None
    ) -> None:
        all_unknown_sids = []
        all_unknown_sids.extend(unknown_sids)
        tx_service = self._node.get_tx_service()

        # retrieving sids of txs with unknown contents
        for tx_hash in unknown_hashes:
            transaction_key = tx_service.get_transaction_key(tx_hash)
            tx_sid = tx_service.get_short_id_by_key(transaction_key)
            all_unknown_sids.append(tx_sid)

        if not self._node.opts.request_recovery:
            if connection is not None:
                network_num = connection.network_num
            else:
                network_num = self._node.network_num
            # log recovery started to match with recovery completing
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_STARTED,
                network_num=network_num,
                txs_count=len(all_unknown_sids),
                more_info="recovery from relay is disabled",
            )
            return

        get_txs_message = GetTxsMessage(short_ids=all_unknown_sids)
        self._node.broadcast(get_txs_message, connection_types=(ConnectionType.RELAY_TRANSACTION,))

        if connection is not None:
            tx_stats.add_txs_by_short_ids_event(
                all_unknown_sids,
                TransactionStatEventType.TX_UNKNOWN_SHORT_IDS_REQUESTED_BY_GATEWAY_FROM_RELAY,
                network_num=self._node.network_num,
                peers=[connection],
                block_hash=convert.bytes_to_hex(block_hash.binary)
            )
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_STARTED,
                network_num=connection.network_num,
                txs_count=len(all_unknown_sids),
                request_hash=convert.bytes_to_hex(
                    crypto.double_sha256(get_txs_message.rawbytes())
                )
            )
        else:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECOVERY_REPEATED,
                network_num=self._node.network_num,
                txs_count=len(all_unknown_sids),
                request_hash=convert.bytes_to_hex(
                    crypto.double_sha256(get_txs_message.rawbytes())
                )
            )

    def schedule_recovery_retry(self, block_awaiting_recovery: BlockRecoveryInfo) -> None:
        """
        Schedules a block recovery attempt. Repeated block recovery attempts result in longer timeouts,
        following `gateway_constants.BLOCK_RECOVERY_INTERVAL_S`'s pattern, until giving up.
        :param block_awaiting_recovery: info about recovering block
        :return:
        """
        block_hash = block_awaiting_recovery.block_hash
        recovery_attempts = self._node.block_recovery_service.recovery_attempts_by_block[block_hash]
        recovery_timed_out = time.time() - block_awaiting_recovery.recovery_start_time >= \
                             self._node.opts.blockchain_block_recovery_timeout_s
        if recovery_attempts >= gateway_constants.BLOCK_RECOVERY_MAX_RETRY_ATTEMPTS or recovery_timed_out:
            logger.error(log_messages.SHORT_ID_RECOVERY_FAIL, block_hash)
            self._node.block_recovery_service.cancel_recovery_for_block(block_hash)
            self._node.block_queuing_service_manager.remove(block_hash)
        else:
            delay = gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[recovery_attempts]
            self._node.alarm_queue.register_approx_alarm(delay, delay / 2, self._trigger_recovery_retry,
                                                         block_awaiting_recovery)

    def _trigger_recovery_retry(self, block_awaiting_recovery: BlockRecoveryInfo) -> None:
        block_hash = block_awaiting_recovery.block_hash
        if self._node.block_recovery_service.awaiting_recovery(block_hash):
            self._node.block_recovery_service.recovery_attempts_by_block[block_hash] += 1
            self.start_transaction_recovery(
                block_awaiting_recovery.unknown_short_ids,
                block_awaiting_recovery.unknown_transaction_hashes,
                block_hash
            )

    def _on_block_decompressed(self, block_msg) -> None:
        pass

    def _validate_block_header_in_block_message(
        self, block_message: AbstractBlockMessage
    ) -> BlockValidationResult:
        block_header_bytes = self._get_block_header_bytes_from_block_message(block_message)
        validation_result = self._validate_block_header(block_header_bytes)

        if not validation_result.is_valid and validation_result.block_hash:
            block_hash = validation_result.block_hash
            assert block_hash is not None
            if block_hash in self._blocks_failed_validation_history:
                block_number = eth_common_utils.block_header_number(block_header_bytes)
                block_difficulty = eth_common_utils.block_header_difficulty(block_header_bytes)
                self.set_last_confirmed_block_parameters(block_number, block_difficulty)

        return validation_result

    def _validate_compressed_block_header(
        self, compressed_block_bytes: Union[bytearray, memoryview]
    ) -> BlockValidationResult:
        block_header_bytes = self._get_compressed_block_header_bytes(compressed_block_bytes)
        validation_result = self._validate_block_header(block_header_bytes)
        if not validation_result.is_valid and validation_result.block_hash:
            block_hash = validation_result.block_hash
            assert block_hash is not None
            self._blocks_failed_validation_history.add(block_hash)
        return validation_result

    def _validate_block_header(self, block_header_bytes: Union[bytearray, memoryview]) -> BlockValidationResult:
        if self._block_validator and self._last_confirmed_block_number and self._last_confirmed_block_difficulty:
            block_validator = self._block_validator
            assert block_validator is not None
            return block_validator.validate_block_header(
                block_header_bytes,
                self._last_confirmed_block_number,
                self._last_confirmed_block_difficulty
            )
        logger.debug(
            "Skipping block validation. Block validator - {}, last confirmed block - {}, last confirmed block difficulty - {}",
            self._block_validator, self._last_confirmed_block_number, self._last_confirmed_block_difficulty)
        return BlockValidationResult(True, None, None)

    def _get_compressed_block_header_bytes(
        self, compressed_block_bytes: Union[bytearray, memoryview]
    ) -> Union[bytearray, memoryview]:
        pass

    def _get_block_header_bytes_from_block_message(
        self, block_message: AbstractBlockMessage
    ) -> Union[bytearray, memoryview]:
        pass
