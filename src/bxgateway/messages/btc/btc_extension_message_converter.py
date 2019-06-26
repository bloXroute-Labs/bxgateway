import typing
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.utils import convert, logger
from bxcommon.utils.object_encoder import ObjectEncoder
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxcommon.utils.proxy.vector_proxy import VectorProxy

from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter, \
    get_block_info, CompactBlockCompressionResult
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage

import task_pool_executor as tpe  # pyre-ignore for now, figure this out later (stub file or Python wrapper?)


class ExtensionCompactBlockRecoveryData(typing.NamedTuple):
    tx_service: ExtensionTransactionService
    mapping_task: tpe.BtcCompactBlockMappingTask


def create_recovered_transactions() -> VectorProxy[tpe.InputBytes, memoryview]:
    encoder = ObjectEncoder(
        lambda input_bytes: memoryview(input_bytes), lambda buf: tpe.InputBytes(buf)
    )
    return VectorProxy(tpe.TransactionList(), encoder)


class BtcExtensionMessageConverter(AbstractBtcMessageConverter):

    DEFAULT_BLOCK_SIZE = 621000

    def __init__(self, btc_magic):
        super(BtcExtensionMessageConverter, self).__init__(btc_magic)
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)
        self.compact_mapping_tasks = TaskQueueProxy(self._create_compact_mapping_task)
        self.decompression_tasks = TaskQueueProxy(self._create_decompression_task)
        self._extension_recovered_items: typing.Dict[int, ExtensionCompactBlockRecoveryData] = {}

    def block_to_bx_block(self, btc_block_msg, tx_service):
        self._default_block_size = max(self._default_block_size, len(btc_block_msg.buf))
        tsk = self.compression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(btc_block_msg.buf), tx_service.proxy)
        task_pool_proxy.run_task(tsk)
        bx_block = tsk.bx_block()
        block = memoryview(bx_block)
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )

        block_info = BlockInfo(
            tsk.txn_count(),
            block_hash,
            tsk.compressed_block_hash().hex_string(),
            tsk.prev_block_hash().hex_string(),
            tsk.short_ids()
        )
        self.compression_tasks.return_task(tsk)
        return block, block_info

    def compact_block_to_bx_block(
            self,
            compact_block: CompactBlockBtcMessage,
            transaction_service: ExtensionTransactionService
    ) -> CompactBlockCompressionResult:
        tsk = self.compact_mapping_tasks.borrow_task()
        tsk.init(tpe.InputBytes(compact_block.buf), transaction_service.proxy, compact_block.magic())
        task_pool_proxy.run_task(tsk)
        success = tsk.success()
        recovered_item = ExtensionCompactBlockRecoveryData(transaction_service, tsk)
        if success:
            result = CompactBlockCompressionResult(
                False, None, None, None, [], create_recovered_transactions()
            )
            return self._extension_recovered_compact_block_to_bx_block(result, recovered_item)
        else:
            recovery_index = self._last_recovery_idx
            self._extension_recovered_items[recovery_index] = recovered_item
            self._last_recovery_idx += 1
            return CompactBlockCompressionResult(
                False,
                None,
                None,
                recovery_index,
                tsk.missing_indices(),
                create_recovered_transactions()
            )

    def recovered_compact_block_to_bx_block(
            self,
            failed_compression_result: CompactBlockCompressionResult,
    ) -> CompactBlockCompressionResult:
        recovered_item = self._extension_recovered_items.pop(failed_compression_result.recovery_index)
        return self._extension_recovered_compact_block_to_bx_block(
            failed_compression_result,
            recovered_item
        )

    def bx_block_to_block(self, bx_block, tx_service):
        tsk = self.decompression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(bx_block), tx_service.proxy)
        task_pool_proxy.run_task(tsk)
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
                "Successfully parsed bx_block broadcast message. {0} transactions in bx_block".format(total_tx_count)
            )
        else:
            btc_block_msg = None
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in bx_block: {2}"
                        .format(len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count))
        block_info = get_block_info(
            bx_block,
            block_hash,
            tsk.short_ids(),
            total_tx_count,
            btc_block_msg
        )
        return btc_block_msg, block_info.block_hash, block_info.short_ids, unknown_tx_sids, unknown_tx_hashes

    def _extension_recovered_compact_block_to_bx_block(
            self,
            compression_result: CompactBlockCompressionResult,
            recovery_item: ExtensionCompactBlockRecoveryData
    ) -> CompactBlockCompressionResult:
        mapping_task = recovery_item.mapping_task
        compression_task: tpe.BtcCompactBlockCompressionTask = mapping_task.compression_task()
        compression_task.add_recovered_transactions(compression_result.recovered_transactions.vector)
        task_pool_proxy.run_task(compression_task)
        bx_block = memoryview(compression_task.bx_block())
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(compression_task.block_hash().hex_string())
        )
        txn_count = compression_task.txn_count()
        compressed_block_hash = compression_task.compressed_block_hash().hex_string()
        prev_block_hash = compression_task.prev_block_hash().hex_string()
        short_ids = compression_task.short_ids()
        block_info = BlockInfo(
            txn_count,
            block_hash,
            compressed_block_hash,
            prev_block_hash,
            short_ids
        )
        self.compact_mapping_tasks.return_task(mapping_task)
        return CompactBlockCompressionResult(True, block_info, bx_block, None, [], [])

    def _create_compression_task(self) -> tpe.BtcBlockCompressionTask:  # pyre-ignore
        return tpe.BtcBlockCompressionTask(self._default_block_size)

    def _create_compact_mapping_task(self) -> tpe.BtcCompactBlockMappingTask:  # pyre-ignore
        return tpe.BtcCompactBlockMappingTask(self._default_block_size)

    def _create_decompression_task(self) -> tpe.BtcBlockDecompressionTask:  # pyre-ignore
        return tpe.BtcBlockDecompressionTask(self._default_block_size)
