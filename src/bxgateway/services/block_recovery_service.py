import time
from collections import defaultdict
from enum import Enum
from typing import Dict, Set, List, NamedTuple

from bxcommon.utils import crypto
from bxcommon.utils.alarm_queue import AlarmQueue
from bxcommon.utils.expiration_queue import ExpirationQueue
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants
from bxutils import logging

logger = logging.get_logger(__name__)


class BlockRecoveryInfo(NamedTuple):
    block_hash: Sha256Hash
    unknown_short_ids: Set[int]
    unknown_transaction_hashes: Set[Sha256Hash]
    recovery_start_time: float


class RecoveredTxsSource(Enum):
    TXS_RECEIVED_FROM_BDN = "TransactionsReceivedFromBdn"
    TXS_RECEIVED_FROM_NODE = "TransactionsReceivedFromNode"
    TXS_RECOVERED = "TxsRecovered"
    COMPRESSED_BLOCK_TXS_RECEIVED = "CompressedBlockTxsReceived"
    BLOCK_RECEIVED_FROM_NODE = "BlockReceivedFromNode"

    def __str__(self):
        return self.name


class BlockRecoveryService:
    """
    Service class that handles blocks gateway receives with unknown transaction short ids are contents.

    Attributes
    ----------
    recovered blocks: queue to which recovered blocks are pushed to
    _alarm_queue: reference to alarm queue to schedule cleanup on

    _bx_block_hash_to_sids: map of compressed block hash to its set of unknown short ids
    _bx_block_hash_to_tx_hashes: map of compressed block hash to its set of unknown transaction hashes
    _bx_block_hash_to_block_hash: map of compressed block hash to its original block hash
    _bx_block_hash_to_block: map of compressed block hash to its compressed byte representation
    _block_hash_to_bx_block_hashes: map of original block hash to compressed block hashes waiting for recovery
    _sid_to_bx_block_hashes: map of short id to compressed block hashes waiting for recovery
    _tx_hash_to_bx_block_hashes: map of transaction hash to block hashes waiting for recovery

    _cleanup_scheduled: whether block recovery has an alarm scheduled to clean up recovering blocks
    _blocks_expiration_queue: queue to trigger expiration of waiting for block recovery
    """

    _alarm_queue: AlarmQueue
    _bx_block_hash_to_sids: Dict[Sha256Hash, Set[int]]
    _bx_block_hash_to_tx_hashes: Dict[Sha256Hash, Set[Sha256Hash]]
    _bx_block_hash_to_block_hash: Dict[Sha256Hash, Sha256Hash]
    _bx_block_hash_to_block: Dict[Sha256Hash, memoryview]
    _block_hash_to_bx_block_hashes: Dict[Sha256Hash, Set[Sha256Hash]]
    _sid_to_bx_block_hashes: Dict[int, Set[Sha256Hash]]
    _tx_hash_to_bx_block_hashes: Dict[Sha256Hash, Set[Sha256Hash]]
    _blocks_expiration_queue: ExpirationQueue
    _cleanup_scheduled: bool = False

    recovery_attempts_by_block: Dict[Sha256Hash, int]

    def __init__(self, alarm_queue: AlarmQueue):
        self.recovered_blocks = []

        self._alarm_queue = alarm_queue

        self._bx_block_hash_to_sids = {}
        self._bx_block_hash_to_tx_hashes = {}
        self._bx_block_hash_to_block_hash = {}
        self._bx_block_hash_to_block = {}
        self.recovery_attempts_by_block = defaultdict(int)
        self._block_hash_to_bx_block_hashes = defaultdict(set)
        self._sid_to_bx_block_hashes = defaultdict(set)
        self._tx_hash_to_bx_block_hashes: Dict[Sha256Hash, Set[Sha256Hash]] = defaultdict(set)
        self._blocks_expiration_queue = ExpirationQueue(gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME)

    def add_block(self, bx_block: memoryview, block_hash: Sha256Hash, unknown_tx_sids: List[int],
                  unknown_tx_hashes: List[Sha256Hash]):
        """
        Adds a block that needs to recovery. Tracks unknown short ids and contents as they come in.
        :param bx_block: bytearray representation of compressed block
        :param block_hash: original ObjectHash of block
        :param unknown_tx_sids: list of unknown short ids
        :param unknown_tx_hashes: list of unknown tx ObjectHashes
        """
        logger.trace("Recovering block with {} unknown short ids and {} contents: {}", len(unknown_tx_sids),
                     len(unknown_tx_hashes), block_hash)
        bx_block_hash = Sha256Hash(crypto.double_sha256(bx_block))

        self._bx_block_hash_to_block[bx_block_hash] = bx_block
        self._bx_block_hash_to_block_hash[bx_block_hash] = block_hash
        self._bx_block_hash_to_sids[bx_block_hash] = set(unknown_tx_sids)
        self._bx_block_hash_to_tx_hashes[bx_block_hash] = set(unknown_tx_hashes)

        self._block_hash_to_bx_block_hashes[block_hash].add(bx_block_hash)
        for sid in unknown_tx_sids:
            self._sid_to_bx_block_hashes[sid].add(bx_block_hash)
        for tx_hash in unknown_tx_hashes:
            self._tx_hash_to_bx_block_hashes[tx_hash].add(bx_block_hash)

        self._blocks_expiration_queue.add(bx_block_hash)
        self._schedule_cleanup()

    def get_blocks_awaiting_recovery(self) -> List[BlockRecoveryInfo]:
        """
        Fetch all blocks still awaiting recovery and retry.
        """
        blocks_awaiting_recovery = []
        for block_hash, bx_block_hashes in self._block_hash_to_bx_block_hashes.items():
            unknown_short_ids = set()
            unknown_transaction_hashes = set()
            for bx_block_hash in bx_block_hashes:
                unknown_short_ids.update(self._bx_block_hash_to_sids[bx_block_hash])
                unknown_transaction_hashes.update(self._bx_block_hash_to_tx_hashes[bx_block_hash])
            blocks_awaiting_recovery.append(BlockRecoveryInfo(block_hash, unknown_short_ids, unknown_transaction_hashes,
                                                              time.time()))
        return blocks_awaiting_recovery

    def check_missing_sid(self, sid: int, recovered_txs_source: RecoveredTxsSource) -> bool:
        """
        Resolves recovering blocks depend on sid.
        :param sid: SID info that has been processed
        :param recovered_txs_source: source of recovered transaction
        """
        if sid in self._sid_to_bx_block_hashes:
            logger.trace("Resolved previously unknown short id: {0}.", sid)

            bx_block_hashes = self._sid_to_bx_block_hashes[sid]
            for bx_block_hash in bx_block_hashes:
                if bx_block_hash in self._bx_block_hash_to_sids:
                    if sid in self._bx_block_hash_to_sids[bx_block_hash]:
                        self._bx_block_hash_to_sids[bx_block_hash].discard(sid)
                        self._check_if_recovered(bx_block_hash, recovered_txs_source)

            del self._sid_to_bx_block_hashes[sid]
            return True
        else:
            return False

    def check_missing_tx_hash(self, tx_hash: Sha256Hash, recovered_txs_source: RecoveredTxsSource) -> bool:
        """
        Resolves recovering blocks depend on transaction hash.
        :param tx_hash: transaction info that has been processed
        :param recovered_txs_source: source of recovered transaction
        """
        if tx_hash in self._tx_hash_to_bx_block_hashes:
            logger.trace("Resolved previously unknown transaction hash {0}.", tx_hash)

            bx_block_hashes = self._tx_hash_to_bx_block_hashes[tx_hash]
            for bx_block_hash in bx_block_hashes:
                if bx_block_hash in self._bx_block_hash_to_tx_hashes:
                    if tx_hash in self._bx_block_hash_to_tx_hashes[bx_block_hash]:
                        self._bx_block_hash_to_tx_hashes[bx_block_hash].discard(tx_hash)
                        self._check_if_recovered(bx_block_hash, recovered_txs_source)

            del self._tx_hash_to_bx_block_hashes[tx_hash]
            return True
        else:
            return False

    def cancel_recovery_for_block(self, block_hash: Sha256Hash) -> bool:
        """
        Cancels recovery for all compressed blocks matching a block hash
        :param block_hash: ObjectHash
        """
        if block_hash in self._block_hash_to_bx_block_hashes:
            logger.trace("Cancelled block recovery for block: {}", block_hash)
            self._remove_recovered_block_hash(block_hash)
            return True
        else:
            return False

    def awaiting_recovery(self, block_hash: Sha256Hash) -> bool:
        return block_hash in self._block_hash_to_bx_block_hashes

    # pyre-fixme[9]: clean_up_time has type `float`; used as `None`.
    def cleanup_old_blocks(self, clean_up_time: float = None):
        """
        Cleans up old compressed blocks awaiting recovery.
        :param clean_up_time:
        """
        logger.debug("Cleaning up block recovery.")
        num_blocks_awaiting_recovery = len(self._bx_block_hash_to_block)
        self._blocks_expiration_queue.remove_expired(current_time=clean_up_time,
                                                     remove_callback=self._remove_not_recovered_block)
        logger.debug("Cleaned up {} blocks awaiting recovery.",
                     num_blocks_awaiting_recovery - len(self._bx_block_hash_to_block))

        if self._bx_block_hash_to_block:
            return gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME

        # disable clean up until receive the next block with unknown tx
        self._cleanup_scheduled = False
        return 0

    def clean_up_recovered_blocks(self):
        """
        Cleans up blocks that have finished recovery.
        :return:
        """
        logger.trace("Cleaning up {} recovered blocks.", len(self.recovered_blocks))
        del self.recovered_blocks[:]

    def _check_if_recovered(self, bx_block_hash: Sha256Hash, recovered_txs_source: RecoveredTxsSource):
        """
        Checks if a compressed block has received all short ids and transaction hashes necessary to recover.
        Adds block to recovered blocks if so.
        :param bx_block_hash: ObjectHash
        :return:
        """
        if self._is_block_recovered(bx_block_hash):
            bx_block = self._bx_block_hash_to_block[bx_block_hash]
            block_hash = self._bx_block_hash_to_block_hash[bx_block_hash]
            logger.debug(
                "Recovery status for block {}, compress block hash {}: "
                "Block recovered by gateway. Source of recovered txs is {}.",
                block_hash, bx_block_hash, recovered_txs_source
            )
            self._remove_recovered_block_hash(block_hash)
            self.recovered_blocks.append((bx_block, recovered_txs_source))

    def _is_block_recovered(self, bx_block_hash: Sha256Hash):
        """
        Indicates if a compressed block has received all short ids and transaction hashes necessary to recover.
        :param bx_block_hash: ObjectHash
        :return:
        """
        return len(self._bx_block_hash_to_sids[bx_block_hash]) == 0 and len(
            self._bx_block_hash_to_tx_hashes[bx_block_hash]) == 0

    def _remove_recovered_block_hash(self, block_hash: Sha256Hash):
        """
        Removes all compressed blocks awaiting recovery that are matches to a recovered block hash.
        :param block_hash: ObjectHash
        :return:
        """
        if block_hash in self._block_hash_to_bx_block_hashes:
            for bx_block_hash in self._block_hash_to_bx_block_hashes[block_hash]:
                if bx_block_hash in self._bx_block_hash_to_block:
                    self._remove_sid_and_tx_mapping_for_bx_block_hash(bx_block_hash)
                    del self._bx_block_hash_to_block[bx_block_hash]
                    del self._bx_block_hash_to_block_hash[bx_block_hash]
            del self._block_hash_to_bx_block_hashes[block_hash]

    def _remove_sid_and_tx_mapping_for_bx_block_hash(self, bx_block_hash: Sha256Hash):
        """
        Removes all short id and transaction mapping for a compressed block.
        :param bx_block_hash:
        :return:
        """
        if bx_block_hash not in self._bx_block_hash_to_block:
            raise ValueError("Can't remove mapping for a block that isn't being recovered.")

        for sid in self._bx_block_hash_to_sids[bx_block_hash]:
            if sid in self._sid_to_bx_block_hashes:
                self._sid_to_bx_block_hashes[sid].discard(bx_block_hash)
                if len(self._sid_to_bx_block_hashes[sid]) == 0:
                    del self._sid_to_bx_block_hashes[sid]
        del self._bx_block_hash_to_sids[bx_block_hash]

        for tx_hash in self._bx_block_hash_to_tx_hashes[bx_block_hash]:
            if tx_hash in self._tx_hash_to_bx_block_hashes:
                self._tx_hash_to_bx_block_hashes[tx_hash].discard(bx_block_hash)
                if len(self._tx_hash_to_bx_block_hashes[tx_hash]) == 0:
                    del self._tx_hash_to_bx_block_hashes[tx_hash]
        del self._bx_block_hash_to_tx_hashes[bx_block_hash]

    def _remove_not_recovered_block(self, bx_block_hash: Sha256Hash):
        """
        Removes compressed block that has not recovered.
        :param bx_block_hash: ObjectHash
        """
        if bx_block_hash in self._bx_block_hash_to_block:
            logger.trace("Block has failed recovery: {}", bx_block_hash)

            self._remove_sid_and_tx_mapping_for_bx_block_hash(bx_block_hash)
            del self._bx_block_hash_to_block[bx_block_hash]

            block_hash = self._bx_block_hash_to_block_hash.pop(bx_block_hash)
            self._block_hash_to_bx_block_hashes[block_hash].discard(bx_block_hash)
            if len(self._block_hash_to_bx_block_hashes[block_hash]) == 0:
                del self._block_hash_to_bx_block_hashes[block_hash]

    def _schedule_cleanup(self):
        if not self._cleanup_scheduled and self._bx_block_hash_to_block:
            logger.trace("Scheduling block recovery cleanup in {} seconds.",
                         gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME)
            self._alarm_queue.register_alarm(gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME, self.cleanup_old_blocks)
            self._cleanup_scheduled = True
