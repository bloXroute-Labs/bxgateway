from collections import deque
from typing import Generic, Deque, Tuple, Dict, TypeVar, Optional, TYPE_CHECKING, List
from abc import ABCMeta, abstractmethod
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)

TMessage = TypeVar("TMessage", bound=AbstractMessage)


class AbstractBlockQueuingService(Generic[TMessage], metaclass=ABCMeta):
    """
    Service managing storage of blocks that are available for sending to blockchain node
    """

    def __init__(self, node: "AbstractGatewayNode"):
        self.node = node

        # queue of tuple (block hash, timestamp) for blocks that need to be sent to blockchain node
        self._block_queue: Deque[Tuple[Sha256Hash, float]] = deque()

        # dictionary with block hash as key and value of tuple ('waiting for recovery' flag, block message)
        self._blocks: Dict[Sha256Hash, Tuple[bool, TMessage]] = {}

        self._blocks_seen_by_blockchain_node: ExpiringSet[Sha256Hash] = \
            ExpiringSet(node.alarm_queue, gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)

    def __len__(self):
        """
        Returns number of items in the queue
        :return: number of items in the queue
        """

        return len(self._block_queue)

    def __contains__(self, block_hash: Sha256Hash):
        """
        Indicates if block hash is in block queue.
        :param block_hash:
        :return: if block hash is in queue
        """
        return block_hash in self._blocks

    @abstractmethod
    def push(self, block_hash: Sha256Hash, block_msg: Optional[TMessage] = None, waiting_for_recovery: bool = False):
        """
        Pushes block to the queue
        :param block_hash: Block hash
        :param block_msg: Block message instance (can be None if waiting for recovery flag is set to True
        :param waiting_for_recovery: flag indicating if gateway is waiting for recovery of the block
        """
        pass

    @abstractmethod
    def remove(self, block_hash: Sha256Hash):
        """
        Removes block from the queue if exists
        :param block_hash: block hash
        """
        pass

    @abstractmethod
    def update_recovered_block(self, block_hash: Sha256Hash, block_msg: TMessage):
        """
        Updates status of the block in the queue as recovered and ready to send to blockchain node

        :param block_hash: block hash
        :param block_msg: recovered block message
        """
        pass

    @abstractmethod
    def mark_blocks_seen_by_blockchain_node(self, block_hashes: List[Sha256Hash]):
        pass

    @abstractmethod
    def mark_block_seen_by_blockchain_node(self, block_hash: Sha256Hash):
        pass

    def can_add_block_to_queuing_service(self, block_hash: Sha256Hash, block_msg: Optional[TMessage] = None,
                                         waiting_for_recovery: bool = False) -> bool:
        if block_msg is None and not waiting_for_recovery:
            raise ValueError("Block message is required if not waiting for recovery of the block.")

        if block_hash in self._blocks:
            raise ValueError("Block with hash {} already exists in the queue.".format(block_hash))

        if block_hash in self._blocks_seen_by_blockchain_node:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_IGNORE_SEEN_BY_BLOCKCHAIN_NODE,
                                                      self.node.network_num)
            return False
        return True

    def _send_block_to_node(self, block_hash: Sha256Hash, block_msg: TMessage):
        logger.info("Forwarding block {} to blockchain node.", block_hash)

        self.node.send_msg_to_node(block_msg)
        handling_time, relay_desc = self.node.track_block_from_bdn_handling_ended(block_hash)
        # if tracking detailed send info, log this event only after all bytes written to sockets
        if not self.node.opts.track_detailed_sent_messages:
            block_stats.add_block_event_by_block_hash(
                block_hash, BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE, network_num=self.node.network_num,
                more_info="{} bytes; Handled in {}; R - {}; {}".format(len(block_msg.rawbytes()),
                                                                       stats_format.duration(handling_time),
                                                                       relay_desc, block_msg.extra_stats_data())
            )
