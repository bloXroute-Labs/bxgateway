import time
import typing
import task_pool_executor as tpe
from datetime import datetime
from typing import Tuple

from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxgateway import ont_constants
from bxgateway.messages.ont.abstract_ont_consensus_message_converter import AbstractOntConsensusMessageConverter
from bxgateway.messages.ont.consensus_ont_message import ConsensusOntMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.errors import message_conversion_error
from bxgateway.utils.ont.ont_object_hash import OntObjectHash
from bxutils import logging

logger = logging.get_logger(__name__)


class OntExtensionConsensusMessageConverter(AbstractOntConsensusMessageConverter):

    DEFAULT_BLOCK_SIZE = ont_constants.ONT_DEFAULT_BLOCK_SIZE
    MINIMAL_SUB_TASK_TX_COUNT = ont_constants.ONT_MINIMAL_SUB_TASK_TX_COUNT

    def __init__(self, ont_magic: int):
        super(OntExtensionConsensusMessageConverter, self).__init__(ont_magic)
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)

    def block_to_bx_block(
            self, block_msg: ConsensusOntMessage, tx_service: TransactionService
    ) -> Tuple[memoryview, BlockInfo]:
        compress_start_datetime = datetime.utcnow()
        compress_start_timestamp = time.time()
        extension_tx_service = typing.cast(ExtensionTransactionService, tx_service)
        self._default_block_size = max(self._default_block_size, len(block_msg.buf))
        tsk = self.compression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(block_msg.buf), extension_tx_service.proxy)
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
            100 - float(compressed_size) / original_size * 100
        )
        self.compression_tasks.return_task(tsk)
        return block, block_info

    def _create_compression_task(self) -> tpe.ConsensusOntBlockCompressionTask:
        return tpe.ConsensusOntBlockCompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)
