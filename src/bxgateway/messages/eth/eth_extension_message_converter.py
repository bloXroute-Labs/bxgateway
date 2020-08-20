import time
import datetime
from typing import Tuple

import task_pool_executor as tpe


from bxcommon.utils import convert, crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.utils.errors import message_conversion_error
from bxgateway.utils.block_info import BlockInfo
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxgateway.abstract_message_converter import BlockDecompressionResult
from bxgateway.messages.eth.eth_abstract_message_converter import EthAbstractMessageConverter
from bxutils import logging

logger = logging.get_logger(__name__)


class EthExtensionMessageConverter(EthAbstractMessageConverter):

    DEFAULT_BLOCK_SIZE = eth_common_constants.ETH_DEFAULT_BLOCK_SIZE
    MINIMAL_SUB_TASK_TX_COUNT = eth_common_constants.ETH_MINIMAL_SUB_TASK_TX_COUNT

    def __init__(self):
        super().__init__()
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)
        self.decompression_tasks = TaskQueueProxy(self._create_decompression_task)

    def block_to_bx_block(
        self,
        block_msg: InternalEthBlockInfo,
        tx_service: ExtensionTransactionService,
        enable_block_compression: bool,
        min_tx_age_seconds: float
    ) -> Tuple[memoryview, BlockInfo]:
        compress_start_datetime = datetime.datetime.utcnow()
        compress_start_timestamp = time.time()
        self._default_block_size = max(self._default_block_size, len(block_msg.rawbytes()))
        tsk = self.compression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(block_msg.rawbytes()), tx_service.proxy, enable_block_compression, min_tx_age_seconds)
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            self.compression_tasks.return_task(tsk)
            raise message_conversion_error.eth_block_compression_error(block_msg.block_hash(), e)
        bx_block = tsk.bx_block()
        starting_offset = tsk.starting_offset()
        block = memoryview(bx_block)[starting_offset:]
        compressed_size = len(block)
        original_size = len(block_msg.rawbytes()) - starting_offset
        block_hash = block_msg.block_hash()

        block_info = BlockInfo(
            block_hash,
            tsk.short_ids(),
            compress_start_datetime,
            datetime.datetime.utcnow(),
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
        decompress_start_datetime = datetime.datetime.utcnow()
        decompress_start_timestamp = time.time()
        tsk = self.decompression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(bx_block_msg), tx_service.proxy)
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            block_hash = Sha256Hash(convert.hex_to_bytes(tsk.block_hash().hex_string()))
            self.decompression_tasks.return_task(tsk)
            # TODO find a better solution
            raise message_conversion_error.eth_block_decompression_error(block_hash, e)

        total_tx_count = tsk.tx_count()
        unknown_tx_hashes = [Sha256Hash(bytearray(unknown_tx_hash.binary()))
                             for unknown_tx_hash in tsk.unknown_tx_hashes()]
        unknown_tx_sids = tsk.unknown_tx_sids()
        block_hash = Sha256Hash(convert.hex_to_bytes(tsk.block_hash().hex_string()))

        if tsk.success():
            starting_offset = tsk.starting_offset()
            block = memoryview(tsk.block_message())[starting_offset:]
            block_msg = InternalEthBlockInfo(block)
            content_size = len(block_msg.rawbytes())
            logger.debug("Successfully parsed block broadcast message. {} "
                         "transactions in block {}", total_tx_count, block_hash)
            bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(bx_block_msg))
            compressed_size = len(bx_block_msg)

            block_info = BlockInfo(
                block_hash,
                tsk.short_ids(),
                decompress_start_datetime,
                datetime.datetime.utcnow(),
                (time.time() - decompress_start_timestamp) * 1000,
                total_tx_count,
                bx_block_hash,
                convert.bytes_to_hex(block_msg.prev_block_hash().binary),
                len(block_msg.rawbytes()),
                compressed_size,
                100 - float(compressed_size) / content_size * 100,
                []
            )
        else:
            block_msg = None

            logger.debug(
                "Block recovery needed for {}. Missing {} sids, {} tx hashes. "
                "Total txs in block: {}",
                block_hash,
                len(unknown_tx_sids),
                len(unknown_tx_hashes),
                total_tx_count
            )
            block_info = BlockInfo(
                block_hash,
                tsk.short_ids(),
                decompress_start_datetime,
                datetime.datetime.utcnow(),
                (time.time() - decompress_start_timestamp) * 1000,
                None,
                None,
                None,
                None,
                None,
                None,
                []
            )

        self.decompression_tasks.return_task(tsk)
        return BlockDecompressionResult(block_msg, block_info, unknown_tx_sids, unknown_tx_hashes)

    def _create_compression_task(self) -> tpe.EthBlockCompressionTask:
        return tpe.EthBlockCompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)

    def _create_decompression_task(self) -> tpe.EthBlockDecompressionTask:
        return tpe.EthBlockDecompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)
