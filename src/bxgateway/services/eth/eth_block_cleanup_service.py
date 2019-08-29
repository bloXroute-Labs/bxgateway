import time

from typing import Iterable
from bxcommon.utils import logger
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.services.eth.abstract_eth_block_cleanup_service import AbstractEthBlockCleanupService


class EthBlockCleanupService(AbstractEthBlockCleanupService):

    """
    Service for managing block cleanup.
    """

    def clean_block_transactions_by_block_components(
            self,
            block_hash: Sha256Hash,
            transactions_list: Iterable[Sha256Hash],
            transaction_service: TransactionService
         ) -> None:
        block_short_ids = []
        block_unknown_tx_hashes = []
        logger.debug("BlockCleanupFlow processing cleanup BlockHash: {}", block_hash)
        start_time = time.time()
        short_ids_count: int = 0
        unknown_tx_hashes_count: int = 0
        for tx_hash in transactions_list:
            short_ids = transaction_service.remove_transaction_by_tx_hash(tx_hash)
            if short_ids is None:
                unknown_tx_hashes_count += 1
                block_unknown_tx_hashes.append(tx_hash)
            else:
                short_ids_count += 1
                block_short_ids.extend(short_ids)
        transaction_service.on_block_cleaned_up(block_hash)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(
            "BlockTransactionsCleanup BlockHash: {} UnknownTxHashes: {} ShortIdCount: {} Duration: {}",
            repr(block_hash), unknown_tx_hashes_count, short_ids_count, duration
        )

        self._block_hash_marked_for_cleanup.remove(block_hash)
        self.node.post_block_cleanup_tasks(
            block_hash=block_hash,
            short_ids=block_short_ids,
            unknown_tx_hashes=block_unknown_tx_hashes
        )
