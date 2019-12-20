import time

from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

from bxcommon.utils import crypto
from bxcommon.services.transaction_service import TransactionService
from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage

from bxgateway.messages.ont.block_ont_message import BlockOntMessage

from bxgateway.services.ont.abstract_ont_block_cleanup_service import AbstractOntBlockCleanupService
from bxgateway.utils.ont.ont_object_hash import OntObjectHash, ONT_HASH_LEN

from bxcommon.services import normal_cleanup_service_helpers

logger = logging.get_logger(LogRecordType.BlockCleanup)


class OntNormalBlockCleanupService(AbstractOntBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    # TODO: Implement block cleanup
    def clean_block_transactions(self, block_msg: BlockOntMessage, transaction_service: TransactionService) -> None:
        raise NotImplementedError()

    def contents_cleanup(self, transaction_service: TransactionService,
                         block_confirmation_message: BlockConfirmationMessage):
        raise NotImplementedError()
