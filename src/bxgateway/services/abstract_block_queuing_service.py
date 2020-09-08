import time
from abc import ABCMeta, abstractmethod
from collections import deque
from typing import (
    Generic,
    Deque,
    Dict,
    TypeVar,
    Optional,
    TYPE_CHECKING,
    List,
    NamedTuple,
    Iterator)

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.expiring_dict import ExpiringDict
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

TBlockMessage = TypeVar("TBlockMessage", bound=AbstractBlockMessage)
THeaderMessage = TypeVar("THeaderMessage", bound=AbstractMessage)


class BlockQueueEntry(NamedTuple):
    block_hash: Sha256Hash
    timestamp: float


class AbstractBlockQueuingService(
    Generic[TBlockMessage, THeaderMessage],
    metaclass=ABCMeta
):
    """
    Service managing storage of blocks that are available for sending to blockchain node
    """

    def __init__(self, node: "AbstractGatewayNode"):
        self.node = node

        # queue of tuple (block hash, timestamp) for blocks that need to be
        # sent to blockchain node
        self._block_queue: Deque[BlockQueueEntry] = deque(
            maxlen=gateway_constants.BLOCK_QUEUE_LENGTH_LIMIT
        )
        self._blocks_waiting_for_recovery: Dict[Sha256Hash, bool] = {}
        self._blocks: ExpiringDict[
            Sha256Hash, Optional[TBlockMessage]
        ] = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "block_queuing_service_blocks"
        )

        self._blocks_seen_by_blockchain_node: ExpiringSet[
            Sha256Hash
        ] = ExpiringSet(
            node.alarm_queue,
            gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S,
            "block_queuing_service_blocks_seen",
        )

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
    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: TBlockMessage
    ) -> THeaderMessage:
        pass

    @abstractmethod
    def update_recovered_block(
        self, block_hash: Sha256Hash, block_msg: TBlockMessage
    ) -> None:
        """
        Updates status of the block in the queue as recovered and ready to send to blockchain node

        :param block_hash: block hash
        :param block_msg: recovered block message
        """
        pass

    @abstractmethod
    def mark_blocks_seen_by_blockchain_node(
        self, block_hashes: List[Sha256Hash]
    ):
        pass

    def block_body_exists(self, block_hash: Sha256Hash) -> bool:
        """
        Returns if the block body exists in _blocks
        :param block_hash: block hash
        :return: if block body exists in _block
        """
        return block_hash in self._blocks

    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        block_message: Optional[TBlockMessage],
        block_number: Optional[int] = None,
    ):
        """
        Marks a block as seen by the blockchain node.

        Currently, block_number is only used for Ethereum.
        """
        if block_message is not None:
            self.store_block_data(block_hash, block_message)

        self._blocks_seen_by_blockchain_node.add(block_hash)
        logger.debug(
            "Blockchain node confirmed receipt of block {}", block_hash
        )

    def push(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[TBlockMessage] = None,
        waiting_for_recovery: bool = False,
    ) -> None:
        """
        Pushes block to the queue
        :param block_hash: Block hash
        :param block_msg: Block message instance (can be None if waiting for recovery flag is set to True
        :param waiting_for_recovery: flag indicating if gateway is waiting for recovery of the block
        """
        if block_msg is None and not waiting_for_recovery:
            raise ValueError(
                "Block message is required if not waiting "
                "for recovery of the block."
            )

        if block_hash in self._blocks:
            raise ValueError(
                "Block with hash {} already exists in the queue.".format(
                    block_hash
                )
            )

        if self.can_add_block_to_queuing_service(block_hash):
            self._block_queue.append(BlockQueueEntry(block_hash, time.time()))
            self._blocks_waiting_for_recovery[block_hash] = waiting_for_recovery
            if block_msg is None:
                self._blocks[block_hash] = None
            else:
                self.store_block_data(block_hash, block_msg)

    def can_add_block_to_queuing_service(self, block_hash: Sha256Hash) -> bool:
        if block_hash in self._blocks_seen_by_blockchain_node:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_IGNORE_SEEN_BY_BLOCKCHAIN_NODE,
                self.node.network_num,
            )
            return False
        return True

    def store_block_data(
        self,
        block_hash: Sha256Hash,
        block_msg: TBlockMessage
    ) -> None:
        self._blocks[block_hash] = block_msg

    def send_block_to_node(
        self, block_hash: Sha256Hash, block_msg: Optional[TBlockMessage] = None
    ) -> None:
        if not self.node.should_process_block_hash(block_hash):
            return

        if block_msg is None:
            block_msg = self._blocks[block_hash]

        logger.info("Forwarding block {} to blockchain node.", block_hash)

        assert block_msg is not None
        self.node.send_msg_to_node(block_msg)
        (
            handling_time,
            relay_desc,
        ) = self.node.track_block_from_bdn_handling_ended(block_hash)

        # if tracking detailed send info, log this event only after all
        # bytes written to sockets
        if not self.node.opts.track_detailed_sent_messages:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE,
                network_num=self.node.network_num,
                more_info="{} bytes; Handled in {}; R - {}; {}".format(
                    len(block_msg.rawbytes()),
                    stats_format.duration(handling_time),
                    relay_desc,
                    block_msg.extra_stats_data(),
                ),
            )

    def try_send_header_to_node(self, block_hash: Sha256Hash) -> bool:
        if block_hash not in self._blocks:
            return False

        block_message = self._blocks[block_hash]
        if block_message is None:
            return False

        header_msg = self.build_block_header_message(block_hash, block_message)
        self.node.send_msg_to_node(header_msg)
        block_stats.add_block_event_by_block_hash(
            block_hash,
            BlockStatEventType.BLOCK_HEADER_SENT_TO_BLOCKCHAIN_NODE,
            network_num=self.node.network_num,
        )
        return True

    def remove_from_queue(self, block_hash: Sha256Hash) -> int:
        """
        Removes block from processing queue, preserving actual block info.

        :return: index of block in queue when removed (-1 if doesn't exist)
        """
        if block_hash not in self._blocks:
            return -1

        logger.trace("Removing block {} from queue.", block_hash)

        for index in range(len(self._block_queue)):
            if self._block_queue[index][0] == block_hash:
                del self._block_queue[index]
                del self._blocks_waiting_for_recovery[block_hash]

                return index

        return -1

    def remove(self, block_hash: Sha256Hash) -> int:
        """
        Removes block from the queue if exists

        :return: index of block in queue when removed (-1 if doesn't exist)
        """

        logger.trace("Purging block {} from queuing service.", block_hash)

        index = self.remove_from_queue(block_hash)
        if block_hash in self._blocks:
            del self._blocks[block_hash]
        return index

    def iterate_recent_block_hashes(
        self,
        max_count: int = gateway_constants.TRACKED_BLOCK_MAX_HASH_LOOKUP
    ) -> Iterator[Sha256Hash]:
        raise NotImplementedError

    def is_node_connection_ready(self) -> bool:
        node_conn = self.node.node_conn
        return node_conn is not None and node_conn.is_active()

    def log_memory_stats(self):
        pass
