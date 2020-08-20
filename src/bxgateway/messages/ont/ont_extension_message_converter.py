import time
from collections import deque
from datetime import datetime
from typing import Tuple, Optional, Set

from bxcommon.utils import convert
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.utils import memory_utils
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxcommon.utils.memory_utils import SpecialTuple
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import ont_constants
from bxgateway.abstract_message_converter import BlockDecompressionResult
from bxgateway.messages.ont import ont_normal_message_converter
from bxgateway.messages.ont.abstract_ont_message_converter import AbstractOntMessageConverter, get_block_info
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.errors import message_conversion_error

from bxutils import logging


import task_pool_executor as tpe

logger = logging.get_logger(__name__)


class OntExtensionMessageConverter(AbstractOntMessageConverter):

    DEFAULT_BLOCK_SIZE = ont_constants.ONT_DEFAULT_BLOCK_SIZE
    MINIMAL_SUB_TASK_TX_COUNT = ont_constants.ONT_MINIMAL_SUB_TASK_TX_COUNT

    def __init__(self, ont_magic: int):
        super(OntExtensionMessageConverter, self).__init__(ont_magic)
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)
        self.decompression_tasks = TaskQueueProxy(self._create_decompression_task)

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
        block_hash = OntObjectHash(binary=convert.hex_to_bytes(tsk.block_hash().hex_string()))

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

    def bx_block_to_block(
            self, bx_block_msg: memoryview, tx_service: ExtensionTransactionService
    ) -> BlockDecompressionResult:
        decompress_start_datetime = datetime.utcnow()
        decompress_start_timestamp = time.time()
        tsk = self.decompression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(bx_block_msg), tx_service.proxy)
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            self.decompression_tasks.return_task(tsk)
            header_info = ont_normal_message_converter.parse_bx_block_header(bx_block_msg, deque())
            raise message_conversion_error.btc_block_decompression_error(header_info.block_hash, e)
        total_tx_count = tsk.tx_count()
        unknown_tx_hashes = [Sha256Hash(bytearray(unknown_tx_hash.binary()))
                             for unknown_tx_hash in tsk.unknown_tx_hashes()]
        unknown_tx_sids = tsk.unknown_tx_sids()
        block_hash = OntObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )
        if tsk.success():
            ont_block_msg = BlockOntMessage(buf=memoryview(tsk.block_message()))
            logger.debug(
                "Successfully parsed block broadcast message. {} transactions "
                "in block {}",
                total_tx_count,
                block_hash
            )
        else:
            ont_block_msg = None
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
            ont_block_msg
        )
        self.decompression_tasks.return_task(tsk)
        return BlockDecompressionResult(ont_block_msg, block_info, unknown_tx_sids, unknown_tx_hashes)

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        return memory_utils.add_special_objects(self.compression_tasks, self.decompression_tasks, ids=ids)

    def _create_compression_task(self) -> tpe.OntBlockCompressionTask:
        return tpe.OntBlockCompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)

    def _create_decompression_task(self) -> tpe.OntBlockDecompressionTask:
        return tpe.OntBlockDecompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)
