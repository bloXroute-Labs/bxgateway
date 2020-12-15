from abc import ABCMeta, abstractmethod
from typing import Set, List, Optional, TYPE_CHECKING

from bxcommon.messages.bloxroute.abstract_cleanup_message import AbstractCleanupMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import Sha256Hash
from bxcommon.utils.memory_utils import SpecialMemoryProperties, SpecialTuple
from bxcommon import constants
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService

from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(LogRecordType.BlockCleanup, __name__)


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
        :param block_queuing_service: block queuing service for node that triggered cleanup
        """
        skip_block_count = self.node.network.block_confirmations_count
        if len(block_hashes) >= skip_block_count:
            tx_service = self.node.get_tx_service(self.network_num)
            tracked_blocks = tx_service.get_tracked_blocks(0, 0)
            confirmed_blocks = block_hashes[:-skip_block_count]
            unconfirmed_blocks = block_hashes[-skip_block_count:]
            if tracked_blocks:
                logger.debug("Block cleanup flow report: tracked blocks: {}, confirmed blocks: {}, "
                             "unconfirmed blocks: {}", tracked_blocks, confirmed_blocks, unconfirmed_blocks)
                cross_match_idx: Optional[int] = None
                for block_hash in reversed(confirmed_blocks):
                    if block_hash in tracked_blocks:
                        tracked_idx = tracked_blocks[block_hash]
                        if cross_match_idx is None or cross_match_idx < tracked_idx:
                            cross_match_idx = tracked_idx
                        logger.trace("Block cleanup flow requested block: {}", block_hash)
                        self.block_cleanup_request(block_hash)
                        self.node.block_queuing_service_manager.remove(block_hash)
                        del tracked_blocks[block_hash]
                    else:
                        logger.trace("Block cleanup flow confirmed block is not tracked: {}", block_hash)
                if cross_match_idx is not None:
                    for tracked_block, idx in tracked_blocks.items():
                        if idx <= cross_match_idx:
                            logger.debug("Tracked block does not exist: {}", tracked_block)
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
                logger.trace("Setting root block hash for cleanup: first block: {} previous block: {}", block_hash,
                             prev_block_hash)

    @abstractmethod
    def clean_block_transactions(
            self,
            block_msg,
            transaction_service: TransactionService
    ) -> None:
        pass

    def block_cleanup_request(self, block_hash: Sha256Hash) -> None:
        if not self.is_marked_for_cleanup(block_hash):
            self._block_hash_marked_for_cleanup.add(block_hash)
            self.last_confirmed_block = block_hash
            block_queuing_service = None
            if self.node.block_queuing_service_manager.is_in_common_block_storage(block_hash):
                block_queuing_service = self.node.block_queuing_service_manager.get_designated_block_queuing_service()
            if block_queuing_service is not None:
                self.node.alarm_queue.register_alarm(
                    constants.MIN_SLEEP_TIMEOUT,
                    self.clean_block_transactions_from_block_queue,
                    block_hash,
                    block_queuing_service
                )
            elif self.node.has_active_blockchain_peer():
                self._request_block(block_hash)
            else:
                logger.debug("Block cleanup for '{}' failed. No connection to node.", repr(block_hash))

    @abstractmethod
    def clean_block_transactions_from_block_queue(
        self, block_hash: Sha256Hash, block_queuing_service
    ):
        pass

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        return super(AbstractBlockCleanupService, self).special_memory_size(ids)

    @abstractmethod
    def contents_cleanup(self,
                         transaction_service: TransactionService,
                         block_confirmation_message: AbstractCleanupMessage
                         ):
        pass

    def process_cleanup_message(
            self,
            msg: AbstractCleanupMessage,
            node: "AbstractGatewayNode",
    ):
        block_cleanup = msg.MESSAGE_TYPE == BloxrouteMessageType.BLOCK_CONFIRMATION
        message_hash = msg.message_hash()
        if not self.node.should_process_block_hash(message_hash):
            return
        transaction_service = node.get_tx_service()
        tracked_blocks = transaction_service.get_tracked_blocks()
        if message_hash in node.block_cleanup_processed_blocks:
            if message_hash in tracked_blocks and block_cleanup:
                logger.debug("Block confirmation message already processed: {}. Reprocessing tracked blocks: {}.",
                             message_hash, tracked_blocks)
            else:
                logger.debug("Cleanup message was already processed. Skipping: {}", message_hash)
                return
        logger.debug("Processing cleanup message: {}", message_hash)
        node.block_cleanup_processed_blocks.add(message_hash)
        self.contents_cleanup(transaction_service, msg)

    @abstractmethod
    def _request_block(self, block_hash: Sha256Hash):
        pass
