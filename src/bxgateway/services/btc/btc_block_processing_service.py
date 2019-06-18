import time
from datetime import datetime

from bxcommon.utils import crypto, logger
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.btc_constants import BTC_SHA_HASH_LEN
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InventoryType


class BtcBlockProcessingService(BlockProcessingService):

    def after_block_processed(self, block_message: BlockBtcMessage):
        """
        Additional processing of the block that can be customized per blockchain
        :param block_message: block message specific to a blockchain
        """
        transaction_service = self._node.get_tx_service()

        # remove transactions that were seen in the previous block
        transaction_service.clean_transaction_hashes_not_seen_in_block()

        for tx in block_message.txns():
            tx_hash = BtcObjectHash(crypto.bitcoin_hash(tx), length=BTC_SHA_HASH_LEN)
            transaction_service.track_seen_transaction_hash(tx_hash)

    def process_compact_block(self, block_message, connection):
        """
        Process compact block for processing on timeout if hold message received.
        If no hold exists, compress and broadcast block immediately.
        :param block_message: compact block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        decompression_start_datetime = datetime.utcnow()
        decompression_start_time = time.time()
        parse_result = connection.message_converter.compact_block_to_bx_block(
            block_message,
            self._node.get_tx_service()
        )
        decompression_end_time = time.time()
        duration = decompression_end_time - decompression_start_time

        missing_indices = parse_result.missing_indices
        missing_indices_count = 0 if missing_indices is None else len(missing_indices)

        if parse_result.success:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_COMPRESSED,
                network_num=connection.network_num,
                start_date_time=decompression_start_datetime,
                end_date_time=decompression_end_time,
                duration=duration,
                success=parse_result.success,
                missing_short_id_count=missing_indices_count,
                txs_count=parse_result.block_info.txn_count,
                prev_block_hash=parse_result.block_info.prev_block_hash,
                more_info="{:.2f}ms".format(
                    duration * 1000
                )
            )

            self._process_and_broadcast_compressed_block(
                parse_result.bx_block,
                connection,
                parse_result.block_info,
                block_hash
            )
        else:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_COMPRESSED,
                network_num=connection.network_num,
                start_date_time=decompression_start_time,
                end_date_time=decompression_end_time,
                duration=duration,
                success=parse_result.success,
                missing_short_id_count=missing_indices_count,
                more_info="{:.2f}ms".format(
                    duration * 1000
                )
            )
            logger.info("Compact block was parsed with {} unknown short ids. Requesting unknown transactions.",
                        len(parse_result.missing_indices))  # pyre-ignore
        return parse_result

    def process_compact_block_recovery(self, msg, recovery_result, connection):
        """
        Process compact block recovery .
        If no hold exists, compress and broadcast block immediately.
        :param msg: compact block recovery message to process
        :param recovery_result:
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = msg.block_hash()
        recovery_start_datetime = datetime.utcnow()
        recovery_start_time = time.time()

        for txn in msg.transactions():
            recovery_result.recovered_transactions.append(txn)

        connection.message_converter.recovered_compact_block_to_bx_block(
            msg,
            recovery_result
        )
        recovery_end_time = time.time()
        duration = recovery_end_time - recovery_start_time

        if recovery_result.success:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_RECOVERY,
                network_num=connection.network_num,
                start_date_time=recovery_start_datetime,
                end_date_time=recovery_end_time,
                duration=duration,
                success=recovery_result.success,
                recoverd_txs_count=len(msg.transactions()),
                txs_count=recovery_result.block_info.txn_count,
                prev_block_hash=recovery_result.block_info.prev_block_hash,
                more_info="{:.2f}ms, {:f}".format(
                    duration * 1000,
                    len(msg.transactions())
                )
            )

            self._process_and_broadcast_compressed_block(
                recovery_result.bx_block,
                connection,
                recovery_result.block_info,
                msg.block_hash()
            )
        else:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_RECOVERY,
                network_num=connection.network_num,
                start_date_time=recovery_start_datetime,
                end_date_time=recovery_end_time,
                duration=duration,
                success=recovery_result.success,
                recoverd_txs_count=len(msg.transactions()),
                more_info="{:.2f}ms, {:f}".format(
                    duration * 1000,
                    len(msg.transactions())
                )
            )
            logger.info(
                "Unable to recover compact block '{}' "
                "after receiving BLOCK TRANSACTIONS message. Requesting full block.",
                msg.block_hash()
            )
            get_data_msg = GetDataBtcMessage(
                    magic=recovery_result.magic,
                    inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())]
            )
            connection.node.send_msg_to_node(get_data_msg)
