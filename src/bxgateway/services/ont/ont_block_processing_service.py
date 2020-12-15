import struct
from typing import cast, Optional

from bxcommon.models.broadcast_message_type import BroadcastMessageType
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.stat_block_type import StatBlockType
from bxgateway import log_messages
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.block_recovery_service import RecoveredTxsSource
from bxgateway.utils.errors.message_conversion_error import MessageConversionError
from bxutils import logging

logger = logging.get_logger(__name__)


class OntBlockProcessingService(BlockProcessingService):

    def __init__(self, node):
        super().__init__(node)
        self._node = cast("bxgateway.connections.ont.ont_gateway_node.OntGatewayNode", node)

    def process_block_broadcast(self, msg, connection: AbstractRelayConnection):
        is_consensus_msg, = struct.unpack_from("?", msg.blob()[8:9])
        if is_consensus_msg != self._node.opts.is_consensus:
            return
        super().process_block_broadcast(msg, connection)

    def retry_broadcast_recovered_blocks(self, connection):
        if self._node.block_recovery_service.recovered_blocks and self._node.opts.has_fully_updated_tx_service:
            for msg, recovered_txs_source in self._node.block_recovery_service.recovered_blocks:
                is_consensus_msg, = struct.unpack_from("?", msg[8:9])
                if is_consensus_msg and self._node.opts.is_consensus:
                    self._handle_decrypted_consensus_block(
                        msg,
                        connection,
                        recovered=True,
                        recovered_txs_source=recovered_txs_source
                    )
                else:
                    self._handle_decrypted_block(
                        msg,
                        connection,
                        recovered=True,
                        recovered_txs_source=recovered_txs_source
                    )

            self._node.block_recovery_service.clean_up_recovered_blocks()

    def _handle_decrypted_block(
        self,
        bx_block: memoryview,
        connection: AbstractRelayConnection,
        encrypted_block_hash_hex: Optional[str] = None,
        recovered: bool = False,
        recovered_txs_source: Optional[RecoveredTxsSource] = None
    ):
        is_consensus_msg, = struct.unpack_from("?", bx_block[8:9])
        if is_consensus_msg and self._node.opts.is_consensus:
            return self._handle_decrypted_consensus_block(
                bx_block,
                connection,
                encrypted_block_hash_hex,
                recovered,
                recovered_txs_source
            )
        else:
            return super()._handle_decrypted_block(
                bx_block,
                connection,
                encrypted_block_hash_hex,
                recovered,
                recovered_txs_source
            )

    def _handle_decrypted_consensus_block(
        self,
        bx_block: memoryview,
        connection: AbstractRelayConnection,
        encrypted_block_hash_hex: Optional[str] = None,
        recovered: bool = False,
        recovered_txs_source: Optional[RecoveredTxsSource] = None
    ):
        transaction_service = self._node.get_tx_service()

        if self._node.has_active_blockchain_peer() or self._node.remote_node_conn:
            try:
                block_message, block_info, unknown_sids, unknown_hashes = \
                    self._node.consensus_message_converter.bx_block_to_block(bx_block, transaction_service)
            except MessageConversionError as e:
                block_stats.add_block_event_by_block_hash(
                    e.msg_hash,
                    BlockStatEventType.BLOCK_CONVERSION_FAILED,
                    network_num=connection.network_num,
                    broadcast_type=BroadcastMessageType.CONSENSUS,
                    conversion_type=e.conversion_type.value
                )
                transaction_service.on_block_cleaned_up(e.msg_hash)
                connection.log_warning(log_messages.FAILED_TO_DECOMPRESS_BLOCK_ONT_CONSENSUS, e.msg_hash, e)
                return
        else:
            connection.log_warning(log_messages.LACK_BLOCKCHAIN_CONNECTION_ONT_CONSENSUS)
            return

        block_hash = block_info.block_hash
        all_sids = block_info.short_ids

        if encrypted_block_hash_hex is not None:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_TO_ENC_BLOCK_MATCH,
                                                      matching_block_hash=encrypted_block_hash_hex,
                                                      matching_block_type=StatBlockType.ENCRYPTED.value,
                                                      network_num=connection.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS)

        self.cancel_hold_timeout(block_hash, connection)

        if recovered:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECOVERY_COMPLETED,
                                                      network_num=connection.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS,
                                                      more_info=str(recovered_txs_source))

        if block_hash in self._node.blocks_seen.contents:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_IGNORE_SEEN,
                                                      start_date_time=block_info.start_datetime,
                                                      end_date_time=block_info.end_datetime,
                                                      network_num=connection.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS,
                                                      prev_block_hash=block_info.prev_block_hash,
                                                      original_size=block_info.original_size,
                                                      compressed_size=block_info.compressed_size,
                                                      txs_count=block_info.txn_count,
                                                      blockchain_network=self._node.opts.blockchain_protocol,
                                                      blockchain_protocol=self._node.opts.blockchain_network,
                                                      matching_block_hash=block_info.compressed_block_hash,
                                                      matching_block_type=StatBlockType.COMPRESSED.value,
                                                      more_info=stats_format.duration(block_info.duration_ms))
            self._node.track_block_from_bdn_handling_ended(block_hash)
            transaction_service.track_seen_short_ids(block_hash, all_sids)
            connection.log_info(
                "Discarding duplicate consensus block {} from the BDN.",
                block_hash
            )
            return

        if not recovered:
            connection.log_info("Received consensus block {} from the BDN.", block_hash)
        else:
            connection.log_info("Successfully recovered consensus block {}.", block_hash)

        if block_message is not None:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_SUCCESS,
                                                      start_date_time=block_info.start_datetime,
                                                      end_date_time=block_info.end_datetime,
                                                      network_num=connection.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS,
                                                      prev_block_hash=block_info.prev_block_hash,
                                                      original_size=block_info.original_size,
                                                      compressed_size=block_info.compressed_size,
                                                      txs_count=block_info.txn_count,
                                                      blockchain_network=self._node.opts.blockchain_protocol,
                                                      blockchain_protocol=self._node.opts.blockchain_network,
                                                      matching_block_hash=block_info.compressed_block_hash,
                                                      matching_block_type=StatBlockType.COMPRESSED.value,
                                                      peer=connection.peer_desc,
                                                      more_info="Consensus compression rate {}, Decompression time {}, "
                                                                "Queued behind {} blocks".format(
                                                          stats_format.percentage(block_info.compression_rate),
                                                          stats_format.duration(block_info.duration_ms),
                                                          self._node.block_queueing_service_manager.get_length_of_each_queuing_service_stats_format()))

            if recovered or block_hash in self._node.block_queueing_service_manager:
                self._node.block_queueing_service_manager.update_recovered_block(block_hash, block_message)
            else:
                self._node.block_queueing_service_manager.push(block_hash, block_message)

            self._node.block_recovery_service.cancel_recovery_for_block(block_hash)
            # self._node.blocks_seen.add(block_hash)
            transaction_service.track_seen_short_ids(block_hash, all_sids)
        else:
            if block_hash in self._node.block_queueing_service_manager and not recovered:
                connection.log_trace("Handling already queued consensus block again. Ignoring.")
                return

            self._node.block_recovery_service.add_block(bx_block, block_hash, unknown_sids, unknown_hashes)
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_DECOMPRESSED_WITH_UNKNOWN_TXS,
                                                      start_date_time=block_info.start_datetime,
                                                      end_date_time=block_info.end_datetime,
                                                      network_num=connection.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS,
                                                      prev_block_hash=block_info.prev_block_hash,
                                                      original_size=block_info.original_size,
                                                      compressed_size=block_info.compressed_size,
                                                      txs_count=block_info.txn_count,
                                                      blockchain_network=self._node.opts.blockchain_protocol,
                                                      blockchain_protocol=self._node.opts.blockchain_network,
                                                      matching_block_hash=block_info.compressed_block_hash,
                                                      matching_block_type=StatBlockType.COMPRESSED.value,
                                                      more_info="{} sids, {} hashes".format(
                                                          len(unknown_sids), len(unknown_hashes)))

            connection.log_info("Consensus block {} requires short id recovery. Querying BDN...", block_hash)

            self.start_transaction_recovery(unknown_sids, unknown_hashes, block_hash, connection)
            if recovered:
                # should never happen –– this should not be called on blocks that have not recovered
                connection.log_error(log_messages.BLOCK_DECOMPRESSION_FAILURE_ONT_CONSENSUS,
                                     block_hash)
            else:
                self._node.block_queueing_service_manager.push(block_hash, waiting_for_recovery=True)
