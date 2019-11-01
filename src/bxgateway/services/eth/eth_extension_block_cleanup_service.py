import task_pool_executor as tpe  # pyre-ignore for now, figure this out later (stub file or Python wrapper?)

from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage
from bxcommon.services import extension_cleanup_service_helpers
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxgateway.services.eth.eth_normal_block_cleanup_service import EthNormalBlockCleanupService
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

logger = logging.get_logger(LogRecordType.BlockCleanup)


def create_cleanup_task() -> tpe.BlockConfirmationCleanupTask:  # pyre-ignore
    return tpe.BlockConfirmationCleanupTask()


# temporarily inherit from normal, until extensions create for ethereum
class EthExtensionBlockCleanupService(EthNormalBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "EthGatewayNode", network_num: int):
        super().__init__(node, network_num)
        self.cleanup_tasks = TaskQueueProxy(create_cleanup_task)

    def contents_cleanup(self,
                         transaction_service: TransactionService,
                         block_confirmation_message: BlockConfirmationMessage
                         ):
        extension_cleanup_service_helpers.contents_cleanup(transaction_service, block_confirmation_message,
                                                           self.cleanup_tasks)
