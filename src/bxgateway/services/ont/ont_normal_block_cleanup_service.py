from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage
from bxcommon.services.transaction_service import TransactionService
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.services.ont.abstract_ont_block_cleanup_service import AbstractOntBlockCleanupService
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

logger = logging.get_logger(LogRecordType.BlockCleanup)


class OntNormalBlockCleanupService(AbstractOntBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    # TODO: Implement block cleanup
    def clean_block_transactions(self, block_msg: BlockOntMessage, transaction_service: TransactionService) -> None:
        raise NotImplementedError()

    # pyre-fixme[14]: `contents_cleanup` overrides method defined in
    #  `AbstractBlockCleanupService` inconsistently.
    def contents_cleanup(self, transaction_service: TransactionService,
                         block_confirmation_message: BlockConfirmationMessage):
        raise NotImplementedError()
