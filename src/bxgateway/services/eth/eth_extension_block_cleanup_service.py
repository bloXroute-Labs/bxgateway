import time
from typing import TYPE_CHECKING, Iterable

import task_pool_executor as tpe  # pyre-ignore for now, figure this out later (stub file or Python wrapper?)

from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage
from bxcommon.services import extension_cleanup_service_helpers
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxgateway.services.eth.abstract_eth_block_cleanup_service import AbstractEthBlockCleanupService
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(LogRecordType.BlockCleanup, __name__)


def create_cleanup_task() -> tpe.BlockConfirmationCleanupTask:  # pyre-ignore
    return tpe.BlockConfirmationCleanupTask()


class EthExtensionBlockCleanupService(AbstractEthBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "EthGatewayNode", network_num: int):
        super().__init__(node, network_num)
        self.cleanup_tasks = TaskQueueProxy(create_cleanup_task)

    def clean_block_transactions_by_block_components(
            self,
            block_hash: Sha256Hash,
            transactions_list: Iterable[Sha256Hash],
            transaction_service: TransactionService
         ) -> None:
        logger.debug("Processing block for cleanup: {}", block_hash)
        start_time = time.time()
        if not isinstance(transactions_list, list):
            transactions_list = list(transactions_list)
        sids = []
        confirmation_msg = BlockConfirmationMessage(
            block_hash, transaction_service.network_num, tx_hashes=transactions_list, sids=sids
        )
        self.contents_cleanup(transaction_service, confirmation_msg)
        self._block_hash_marked_for_cleanup.remove(block_hash)
        end_time = time.time()
        duration = end_time - start_time
        logger.debug(
            "Finished cleaning up block {}. Processed {} hashes. Took {:.3f}s.",
            block_hash,
            len(transactions_list),
            duration
        )
        self.node.post_block_cleanup_tasks(
            block_hash,
            sids,
            transactions_list
        )

    def contents_cleanup(
            self,
            transaction_service: TransactionService,
            block_confirmation_message: BlockConfirmationMessage
     ):
        extension_cleanup_service_helpers.contents_cleanup(
            transaction_service, block_confirmation_message, self.cleanup_tasks
        )
