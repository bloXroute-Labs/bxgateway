import typing
from datetime import datetime

from bxcommon.connections.connection_type import ConnectionType
from bxutils import logging
from bxgateway import log_messages

from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats

from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.messages.btc.abstract_btc_message_converter import CompactBlockCompressionResult
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.block_transactions_btc_message import BlockTransactionsBtcMessage
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InventoryType
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.utils.errors.message_conversion_error import MessageConversionError

logger = logging.get_logger(__name__)


class BtcBlockProcessingService(BlockProcessingService):

    def process_compact_block(
            self, block_message: CompactBlockBtcMessage, connection: BtcNodeConnection
    ) -> CompactBlockCompressionResult:
        """
        Process compact block for processing on timeout if hold message received.
        If no hold exists, compress and broadcast block immediately.
        :param block_message: compact block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        parse_result = self._node.message_converter.compact_block_to_bx_block(  # pyre-ignore
            block_message,
            self._node.get_tx_service()
        )
        block_info = parse_result.block_info
        if parse_result.success:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_COMPRESSED_SUCCESS,
                network_num=connection.network_num,
                start_date_time=block_info.start_datetime,
                end_date_time=block_info.end_datetime,
                duration=block_info.duration_ms / 1000,
                success=parse_result.success,
                txs_count=parse_result.block_info.txn_count,
                prev_block_hash=parse_result.block_info.prev_block_hash,
                original_size=block_info.original_size,
                more_info="Compression: {}->{} bytes, {}, {}; Tx count: {}".format(
                    block_info.original_size,
                    block_info.compressed_size,
                    stats_format.percentage(block_info.compression_rate),
                    stats_format.duration(block_info.duration_ms),
                    block_info.txn_count)
            )
            self._node.block_cleanup_service.on_new_block_received(block_hash, block_message.prev_block_hash())
            self._process_and_broadcast_compressed_block(
                parse_result.bx_block,
                connection,
                parse_result.block_info,
                block_hash
            )
        else:
            missing_indices = parse_result.missing_indices
            missing_indices_count = 0 if missing_indices is None else len(missing_indices)
            start_datetime = block_info.start_datetime
            end_datetime = datetime.utcnow()
            duration = (end_datetime - start_datetime).total_seconds()
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_COMPRESSED_FAILED,
                network_num=connection.network_num,
                start_date_time=start_datetime,
                end_date_time=end_datetime,
                duration=duration,
                success=parse_result.success,
                missing_short_id_count=missing_indices_count,
                more_info="{:.2f}ms".format(
                    duration * 1000
                )
            )
            logger.warning(log_messages.UNKNOWN_SHORT_IDS,
                           missing_indices_count)
        return parse_result

    def process_compact_block_recovery(
            self,
            msg: BlockTransactionsBtcMessage,
            failure_result: CompactBlockCompressionResult,
            connection: BtcNodeConnection
    ) -> None:
        """
        Process compact block recovery .
        If no hold exists, compress and broadcast block immediately.
        """
        block_hash = msg.block_hash()

        for txn in msg.transactions():
            failure_result.recovered_transactions.append(txn)

        try:
            recovery_result = self._node.message_converter.recovered_compact_block_to_bx_block(  # pyre-ignore
                failure_result
            )
        except MessageConversionError as e:
            block_stats.add_block_event_by_block_hash(
                e.msg_hash,
                BlockStatEventType.BLOCK_CONVERSION_FAILED,
                network_num=connection.network_num,
                conversion_type=e.conversion_type.value
            )
            logger.warning(log_messages.COMPACT_BLOCK_PROCESSING_FAIL,
                           e.msg_hash, e)
            get_data_msg = GetDataBtcMessage(
                magic=msg.magic(),
                inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())]
            )
            connection.enqueue_msg(get_data_msg)
            return
        block_info = recovery_result.block_info

        if recovery_result.success:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_RECOVERY_SUCCESS,
                network_num=connection.network_num,
                start_date_time=block_info.start_datetime,
                end_date_time=block_info.end_datetime,
                duration=block_info.duration_ms / 1000,
                success=recovery_result.success,
                recoverd_txs_count=len(msg.transactions()),
                txs_count=recovery_result.block_info.txn_count,
                prev_block_hash=recovery_result.block_info.prev_block_hash,
                more_info="{:.2f}ms, {:f}".format(
                    block_info.duration_ms,
                    len(msg.transactions())
                )
            )
            prev_block = Sha256Hash(convert.hex_to_bytes(block_info.prev_block_hash))
            self._node.block_cleanup_service.on_new_block_received(block_hash, prev_block)
            self._process_and_broadcast_compressed_block(
                recovery_result.bx_block,
                connection,
                recovery_result.block_info,
                msg.block_hash()
            )
        else:
            start_datetime = block_info.start_datetime
            end_datetime = datetime.utcnow()
            duration = (end_datetime - start_datetime).total_seconds()
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_RECOVERY_FAILED,
                network_num=connection.network_num,
                start_date_time=start_datetime,
                end_date_time=end_datetime,
                duration=duration,
                success=recovery_result.success,
                recoverd_txs_count=len(msg.transactions()),
                more_info="{:.2f}ms, {:f}".format(
                    duration * 1000,
                    len(msg.transactions())
                )
            )
            logger.warning(log_messages.COMPACT_BLOCK_RECOVERY_FAIL, msg.block_hash())
            get_data_msg = GetDataBtcMessage(
                magic=msg.magic(),
                inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())]
            )
            connection.enqueue_msg(get_data_msg)

    def _on_block_decompressed(self, block_msg):
        msg = typing.cast(BlockBtcMessage, block_msg)
        self._node.block_cleanup_service.on_new_block_received(msg.block_hash(), msg.prev_block_hash())
