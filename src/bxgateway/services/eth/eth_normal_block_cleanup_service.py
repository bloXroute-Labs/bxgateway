import time
from typing import Iterable

from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

from bxcommon.services import normal_cleanup_service_helpers
from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.services.eth.abstract_eth_block_cleanup_service import AbstractEthBlockCleanupService

logger = logging.get_logger(LogRecordType.BlockCleanup, __name__)


class EthNormalBlockCleanupService(AbstractEthBlockCleanupService):

    """
    Service for managing block cleanup.
    """

    def clean_block_transactions_by_block_components(
            self,
            block_hash: Sha256Hash,
            transactions_list: Iterable[Sha256Hash],
            transaction_service: TransactionService
         ) -> None:
        logger.debug("Processing block for cleanup: {}", block_hash)
        tx_hash_to_contents_len_before_cleanup = transaction_service.get_tx_hash_to_contents_len()
        short_id_count_before_cleanup = transaction_service.get_short_id_count()

        block_short_ids = []
        block_unknown_tx_hashes = []

        short_ids_count = 0
        unknown_tx_hashes_count = 0
        transactions_processed = 0

        start_time = time.time()
        for tx_hash in transactions_list:
            short_ids = transaction_service.remove_transaction_by_tx_hash(tx_hash, force=True)
            if short_ids is None:
                unknown_tx_hashes_count += 1
                block_unknown_tx_hashes.append(tx_hash)
            else:
                short_ids_count += len(short_ids)
                block_short_ids.extend(short_ids)
            transactions_processed += 1
        transaction_service.on_block_cleaned_up(block_hash)

        end_time = time.time()
        duration = end_time - start_time
        tx_hash_to_contents_len_after_cleanup = transaction_service.get_tx_hash_to_contents_len()
        short_id_count_after_cleanup = transaction_service.get_short_id_count()

        logger.debug(
            "Finished cleaning up block {}. Processed {} hashes, {} of which were unknown, and cleaned up {} "
            "short ids. Took {:.3f}s.",
            block_hash, transactions_processed, unknown_tx_hashes_count, short_ids_count, duration
        )

        transaction_service.log_block_transaction_cleanup_stats(block_hash, transactions_processed,
                                                                tx_hash_to_contents_len_before_cleanup,
                                                                tx_hash_to_contents_len_after_cleanup,
                                                                short_id_count_before_cleanup,
                                                                short_id_count_after_cleanup)

        self._block_hash_marked_for_cleanup.discard(block_hash)
        self.node.post_block_cleanup_tasks(
            block_hash,
            block_short_ids,
            block_unknown_tx_hashes
        )

    # pyre-fixme[14]: `contents_cleanup` overrides method defined in
    #  `AbstractBlockCleanupService` inconsistently.
    def contents_cleanup(self,
                         transaction_service: TransactionService,
                         block_confirmation_message: BlockConfirmationMessage
                         ):
        normal_cleanup_service_helpers.contents_cleanup(transaction_service, block_confirmation_message)
