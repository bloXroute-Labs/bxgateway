from typing import Set, List, Optional
from abc import ABCMeta, abstractmethod

from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.memory_utils import SpecialMemoryProperties, SpecialTuple
from bxcommon.utils import memory_utils

from bxgateway.utils.btc.btc_object_hash import Sha256Hash

logger = logging.get_logger(LogRecordType.BlockCleanup)


class AbstractBlockCleanupService(SpecialMemoryProperties, metaclass=ABCMeta):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "AbstractGatewayNode", network_num: int):
        """
        Constructor
        :param node: reference to node object
        :param network_num: network number
        """

        self.node: "AbstractGatewayNode" = node
        self.network_num: int = network_num

        self._block_hash_marked_for_cleanup: Set[Sha256Hash] = set()
        self.last_confirmed_block: Optional[Sha256Hash] = None

    def is_marked_for_cleanup(self, block_hash: Sha256Hash) -> bool:
        return block_hash in self._block_hash_marked_for_cleanup

    def mark_blocks_and_request_cleanup(self, block_hashes: List[Sha256Hash]) -> None:
        """
        mark already seen blocks sent again from the bitcoin node for cleanup.
        compare the inventory list against our tracked seen blocks in transaction service.
        skipping the # most recent blocks (# refers to block_confirmations_count opt - 1).
        we explicitly request the inventory list from the bitcoin node
        starting from the last block confirmed for cleanup, up to the most recent block
        (or the following 500 in the chain (whichever comes first)).
        the comparison attempts to cross a match between the btc node list and our list,
        starting from the most recent block (skipping the # count), hence the cross_match_idx.
        the cross_match_idx is the index in which we start to mark our tracked blocks for cleanup.
        an example:
        last_confirmed_block = b0
        block_confirmations_count = 6
        block_hashes = [b1, b2, b3, b4, b5, b6, b7, b8, b9]
        tracked_seen_blocks = [b1, b2, b2.1 (fork), b3, b4, b5, b6, b7, b8, b9]
        cross_match_idx = 4
        blocks marked for full cleanup = [b1, b2, b3, b4]
        blocks removed from tracking = [b2.1]
        :param block_hashes: a list of block hashes from a standard inventory message
        """
        skip_block_count = self.node.network.block_confirmations_count
        if len(block_hashes) >= skip_block_count:
            tx_service = self.node.get_tx_service(self.network_num)
            tracked_blocks = tx_service.get_tracked_blocks(0, 0)
            confirmed_blocks = block_hashes[:-skip_block_count]
            unconfirmed_blocks = block_hashes[-skip_block_count:]
            if tracked_blocks:
                logger.info("BlocksMarkedForCleanup TrackedBlocks: {} ConfirmedBlocks: {} UnConfirmedBlocks: {}",
                            tracked_blocks, confirmed_blocks, unconfirmed_blocks,
                )
                cross_match_idx: Optional[int] = None
                for block_hash in reversed(confirmed_blocks):
                    if block_hash in tracked_blocks:
                        tracked_idx = tracked_blocks[block_hash]
                        if cross_match_idx is None or cross_match_idx < tracked_idx:
                            cross_match_idx = tracked_idx
                        logger.debug("BlockCleanupFlow request block cleanup BlockHash: {}", block_hash)
                        self.block_cleanup_request(block_hash)
                        del tracked_blocks[block_hash]
                    else:
                        logger.debug("BlockCleanupFlow confirmed block is not tracked BlockHash: {}", block_hash)
                if cross_match_idx is not None:
                    for tracked_block, idx in tracked_blocks.items():
                        if idx <= cross_match_idx:
                            logger.info("TrackedBlockNotExists BlockHash: {}", repr(tracked_block))
                            tx_service.on_block_cleaned_up(tracked_block)

    def on_new_block_received(self, block_hash: Sha256Hash, prev_block_hash: Sha256Hash) -> None:
        """
        checks if there are no confirmed blocks for cleanup.
        if True, perform a second check if there are no tracked blocks in tx service
        and add the prev block to the tracker if so.
        this is needed because the request for blocks return only the initial block's children.
        :param block_hash: the current block hash
        :param prev_block_hash: the prev block hash
        """
        if self.last_confirmed_block is None:
            tx_service = self.node.get_tx_service(self.network_num)
            if tx_service.get_tracked_seen_block_count() == 0:
                tx_service.track_seen_short_ids(prev_block_hash, [])
                logger.debug("RootBlockTrackedSeen FirstBlockHash: {} PrevBlock: {}", block_hash, prev_block_hash)

    @abstractmethod
    def clean_block_transactions(
            self,
            block_msg,
            transaction_service: TransactionService
    ) -> None:
        pass

    @abstractmethod
    def block_cleanup_request(self, block_hash: Sha256Hash) -> None:
        pass

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        obj_size = memory_utils.get_object_size(self)
        return SpecialTuple(obj_size.size, ids)
