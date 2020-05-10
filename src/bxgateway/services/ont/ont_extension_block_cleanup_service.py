import typing
import time
from typing import TYPE_CHECKING, Optional, Set
from datetime import datetime

from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.memory_utils import SpecialTuple
from bxcommon.utils import memory_utils
from bxcommon.services import extension_cleanup_service_helpers

from bxgateway import ont_constants
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.services.ont.abstract_ont_block_cleanup_service import AbstractOntBlockCleanupService

import task_pool_executor as tpe

if TYPE_CHECKING:
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode

logger = logging.get_logger(LogRecordType.BlockCleanup, __name__)


def create_block_confirmation_cleanup_task() -> tpe.BlockConfirmationCleanupTask:
    return tpe.BlockConfirmationCleanupTask()


class OntExtensionBlockCleanupService(AbstractOntBlockCleanupService):

    MINIMAL_SUB_TASK_TX_COUNT = ont_constants.ONT_MINIMAL_SUB_TASK_TX_COUNT

    def __init__(self, node: "OntGatewayNode", network_num: int):
        super(OntExtensionBlockCleanupService, self).__init__(node, network_num)
        self.block_cleanup_tasks = TaskQueueProxy(self._create_block_cleanup_task)
        self.block_confirmation_cleanup_tasks = TaskQueueProxy(create_block_confirmation_cleanup_task)

    def clean_block_transactions(
            self, block_msg: BlockOntMessage, transaction_service: TransactionService
    ) -> None:
        start_datetime = datetime.utcnow()
        start_time = time.time()
        tx_hash_to_contents_len_before_cleanup = transaction_service.get_tx_hash_to_contents_len()
        cleanup_task = self.block_cleanup_tasks.borrow_task()
        tx_service = typing.cast(ExtensionTransactionService, transaction_service)
        cleanup_task.init(tpe.InputBytes(block_msg.buf), tx_service.proxy)
        init_time = time.time()
        task_pool_proxy.run_task(cleanup_task)
        task_run_time = time.time()
        unknown_tx_hashes_count = len(cleanup_task.unknown_tx_hashes())
        tx_property_fetch_time = time.time()
        short_ids = cleanup_task.short_ids()
        short_ids_fetch_time = time.time()
        short_ids_count = len(short_ids)
        tx_service.update_removed_transactions(cleanup_task.total_content_removed(), short_ids)
        remove_from_tx_service_time = time.time()
        # TODO : clean the short ids/transactions from the alarm queue after refactoring the transaction service
        block_hash = block_msg.block_hash()
        tx_service.on_block_cleaned_up(block_hash)
        tx_hash_to_contents_len_after_cleanup = transaction_service.get_tx_hash_to_contents_len()
        end_datetime = datetime.utcnow()
        end_time = time.time()

        logger.statistics(
            {
                "type": "BlockTransactionsCleanup",
                "block_hash": repr(block_hash),
                "unknown_tx_hashes_count": unknown_tx_hashes_count,
                "short_ids_count": short_ids_count,
                "block_transactions_count": cleanup_task.txn_count(),
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "task_init_time": init_time - start_time,
                "task_run_time": task_run_time - init_time,
                "tx_property_fetch_time": tx_property_fetch_time - task_run_time,
                "short_ids_fetch_time": short_ids_fetch_time - tx_property_fetch_time,
                "remove_from_tx_service_time": remove_from_tx_service_time - short_ids_fetch_time,
                "duration": end_time - start_time,
                "tx_hash_to_contents_len_before_cleanup": tx_hash_to_contents_len_before_cleanup,
                "tx_hash_to_contents_len_after_cleanup": tx_hash_to_contents_len_after_cleanup,
            }
        )
        self.block_cleanup_tasks.return_task(cleanup_task)
        self._block_hash_marked_for_cleanup.discard(block_hash)
        self.node.post_block_cleanup_tasks(
            block_hash=block_hash,
            short_ids=short_ids,
            unknown_tx_hashes=(
                Sha256Hash(convert.hex_to_bytes(tx_hash.hex_string()))
                for tx_hash in cleanup_task.unknown_tx_hashes()
            )
        )

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        return memory_utils.add_special_objects(
            self.block_cleanup_tasks, self.block_confirmation_cleanup_tasks, ids
        )

    # pyre-fixme[14]: `contents_cleanup` overrides method defined in
    #  `AbstractBlockCleanupService` inconsistently.
    def contents_cleanup(
        self,
        transaction_service: TransactionService,
        block_confirmation_message: BlockConfirmationMessage
    ):
        extension_cleanup_service_helpers.contents_cleanup(
            transaction_service,
            block_confirmation_message,
            self.block_confirmation_cleanup_tasks
        )

    def _create_block_cleanup_task(self) -> tpe.OntBlockCleanupTask:
        return tpe.OntBlockCleanupTask(self.MINIMAL_SUB_TASK_TX_COUNT)
