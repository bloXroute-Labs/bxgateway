import time
from collections import deque
from datetime import datetime
from typing import Tuple, Optional, Dict, NamedTuple, Set

from bxutils import logging

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import Sha256Hash, BtcObjectHash
from bxcommon.utils.object_encoder import ObjectEncoder
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxcommon.utils.proxy.vector_proxy import VectorProxy
from bxcommon.utils.memory_utils import SpecialTuple
from bxcommon.utils import memory_utils

from bxgateway.messages.btc import btc_normal_message_converter
from bxgateway import btc_constants
from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter, get_block_info, \
    CompactBlockCompressionResult
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.utils.errors import message_conversion_error
from bxgateway.abstract_message_converter import BlockDecompressionResult

import task_pool_executor as tpe

logger = logging.get_logger(__name__)


class ExtensionCompactBlockRecoveryData(NamedTuple):
    tx_service: ExtensionTransactionService
    mapping_task: tpe.BtcCompactBlockMappingTask


def create_recovered_transactions() -> VectorProxy[tpe.InputBytes, memoryview]:
    encoder = ObjectEncoder(
        lambda input_bytes: memoryview(input_bytes), lambda buf: tpe.InputBytes(buf)
    )
    return VectorProxy(tpe.TransactionList(), encoder)


class BtcExtensionMessageConverter(AbstractBtcMessageConverter):

    DEFAULT_BLOCK_SIZE = btc_constants.BTC_DEFAULT_BLOCK_SIZE
    MINIMAL_SUB_TASK_TX_COUNT = btc_constants.BTC_MINIMAL_SUB_TASK_TX_COUNT

    def __init__(self, btc_magic):
        super(BtcExtensionMessageConverter, self).__init__(btc_magic)
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)
        self.compact_mapping_tasks = TaskQueueProxy(self._create_compact_mapping_task)
        self.decompression_tasks = TaskQueueProxy(self._create_decompression_task)
        self._extension_recovered_items: Dict[int, ExtensionCompactBlockRecoveryData] = {}

    def block_to_bx_block(
        self, block_msg, tx_service, enable_block_compression: bool, min_tx_age_seconds: float
    ) -> Tuple[memoryview, BlockInfo]:
        compress_start_datetime = datetime.utcnow()
        compress_start_timestamp = time.time()
        self._default_block_size = max(self._default_block_size, len(block_msg.buf))
        tsk = self.compression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(block_msg.buf), tx_service.proxy, enable_block_compression, min_tx_age_seconds)
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            self.compression_tasks.return_task(tsk)
            raise message_conversion_error.btc_block_compression_error(block_msg.block_hash(), e)
        bx_block = tsk.bx_block()
        block = memoryview(bx_block)
        compressed_size = len(block)
        original_size = len(block_msg.rawbytes())
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )

        block_info = BlockInfo(
            block_hash,
            tsk.short_ids(),
            compress_start_datetime,
            datetime.utcnow(),
            (time.time() - compress_start_timestamp) * 1000,
            tsk.txn_count(),
            tsk.compressed_block_hash().hex_string(),
            tsk.prev_block_hash().hex_string(),
            original_size,
            compressed_size,
            100 - float(compressed_size) / original_size * 100,
            tsk.ignored_short_ids()
        )
        self.compression_tasks.return_task(tsk)
        return block, block_info

    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        decompress_start_datetime = datetime.utcnow()
        decompress_start_timestamp = time.time()
        tsk = self.decompression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(bx_block_msg), tx_service.proxy)
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            self.decompression_tasks.return_task(tsk)
            header_info = btc_normal_message_converter.parse_bx_block_header(bx_block_msg, deque())
            raise message_conversion_error.btc_block_decompression_error(header_info.block_hash, e)
        total_tx_count = tsk.tx_count()
        unknown_tx_hashes = [Sha256Hash(bytearray(unknown_tx_hash.binary()))
                             for unknown_tx_hash in tsk.unknown_tx_hashes()]
        unknown_tx_sids = tsk.unknown_tx_sids()
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )
        if tsk.success():
            btc_block_msg = BlockBtcMessage(buf=memoryview(tsk.block_message()))
            logger.debug(
                "Successfully parsed block broadcast message. {} transactions "
                "in block {}",
                total_tx_count,
                block_hash
            )
        else:
            btc_block_msg = None
            logger.debug(
                "Block recovery needed for {}. Missing {} sids, {} tx hashes. "
                "Total txs in block: {}",
                block_hash,
                len(unknown_tx_sids),
                len(unknown_tx_hashes),
                total_tx_count
            )
        block_info = get_block_info(
            bx_block_msg,
            block_hash,
            tsk.short_ids(),
            decompress_start_datetime,
            decompress_start_timestamp,
            total_tx_count,
            btc_block_msg
        )
        self.decompression_tasks.return_task(tsk)
        return BlockDecompressionResult(btc_block_msg, block_info, unknown_tx_sids, unknown_tx_hashes)

    # pyre-fixme[14]: `compact_block_to_bx_block` overrides method defined in
    #  `AbstractBtcMessageConverter` inconsistently.
    def compact_block_to_bx_block(
        self,
        compact_block: CompactBlockBtcMessage,
        transaction_service: ExtensionTransactionService
    ) -> CompactBlockCompressionResult:
        compress_start_datetime = datetime.utcnow()
        tsk = self.compact_mapping_tasks.borrow_task()
        tsk.init(tpe.InputBytes(compact_block.buf), transaction_service.proxy, compact_block.magic())
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            self.compact_mapping_tasks.return_task(tsk)
            raise message_conversion_error.btc_compact_block_compression_error(compact_block.block_hash(), e)
        success = tsk.success()
        recovered_item = ExtensionCompactBlockRecoveryData(transaction_service, tsk)
        block_info = BlockInfo(
            compact_block.block_hash(),
            [],
            compress_start_datetime,
            compress_start_datetime,
            0,
            None,
            None,
            None,
            len(compact_block.rawbytes()),
            None,
            None,
            []
        )
        if success:
            result = CompactBlockCompressionResult(
                False, block_info, None, None, [], create_recovered_transactions()
            )
            return self._extension_recovered_compact_block_to_bx_block(result, recovered_item)
        else:
            recovery_index = self._last_recovery_idx
            self._extension_recovered_items[recovery_index] = recovered_item
            self._last_recovery_idx += 1
            return CompactBlockCompressionResult(
                False,
                block_info,
                None,
                recovery_index,
                tsk.missing_indices(),
                create_recovered_transactions()
            )

    def recovered_compact_block_to_bx_block(  # pyre-ignore
            self,
            failed_mapping_result: CompactBlockCompressionResult,
    ) -> CompactBlockCompressionResult:
        failed_block_info = failed_mapping_result.block_info
        block_info = BlockInfo(
            failed_block_info.block_hash,  # pyre-ignore
            failed_block_info.short_ids,  # pyre-ignore
            datetime.utcnow(),
            datetime.utcnow(),
            0,
            None,
            None,
            None,
            failed_block_info.original_size,  # pyre-ignore
            None,
            None,
            []
        )
        failed_mapping_result.block_info = block_info
        recovered_item = self._extension_recovered_items.pop(failed_mapping_result.recovery_index)  # pyre-ignore
        return self._extension_recovered_compact_block_to_bx_block(
            failed_mapping_result,
            recovered_item
        )

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        return memory_utils.add_special_objects(
            self.compression_tasks, self.decompression_tasks, self.compact_mapping_tasks, ids=ids
        )

    def _extension_recovered_compact_block_to_bx_block(
        self,
        mapping_result: CompactBlockCompressionResult,
        recovery_item: ExtensionCompactBlockRecoveryData
    ) -> CompactBlockCompressionResult:
        mapping_task = recovery_item.mapping_task
        compression_task: tpe.BtcCompactBlockCompressionTask = mapping_task.compression_task()
        # pyre-fixme[16]: `List` has no attribute `vector`.
        compression_task.add_recovered_transactions(mapping_result.recovered_transactions.vector)
        mapping_block_info = mapping_result.block_info
        try:
            task_pool_proxy.run_task(compression_task)
        except tpe.AggregatedException as e:
            self.compact_mapping_tasks.return_task(mapping_task)
            # pyre-fixme[16]: `Optional` has no attribute `block_hash`.
            raise message_conversion_error.btc_compact_block_compression_error(mapping_block_info.block_hash, e)
        bx_block = memoryview(compression_task.bx_block())
        block_hash = mapping_block_info.block_hash
        txn_count = compression_task.txn_count()
        compressed_block_hash = compression_task.compressed_block_hash().hex_string()
        prev_block_hash = compression_task.prev_block_hash().hex_string()
        short_ids = compression_task.short_ids()
        compress_end_datetime = datetime.utcnow()
        # pyre-fixme[16]: `Optional` has no attribute `start_datetime`.
        compress_start_datetime = mapping_block_info.start_datetime
        # pyre-fixme[16]: `Optional` has no attribute `original_size`.
        original_size = mapping_block_info.original_size
        compressed_size = len(bx_block)
        block_info = BlockInfo(
            block_hash,
            short_ids,
            compress_start_datetime,
            compress_end_datetime,
            (compress_end_datetime - compress_start_datetime).total_seconds() * 1000,
            txn_count,
            compressed_block_hash,
            prev_block_hash,
            original_size,
            compressed_size,
            100 - float(compressed_size) / original_size * 100,
            []
        )
        self.compact_mapping_tasks.return_task(mapping_task)
        return CompactBlockCompressionResult(True, block_info, bx_block, None, [], [])

    def _create_compression_task(self) -> tpe.BtcBlockCompressionTask:
        return tpe.BtcBlockCompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)

    def _create_decompression_task(self) -> tpe.BtcBlockDecompressionTask:
        return tpe.BtcBlockDecompressionTask(
            self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT
        )

    def _create_compact_mapping_task(self) -> tpe.BtcCompactBlockMappingTask:
        return tpe.BtcCompactBlockMappingTask(self._default_block_size)
