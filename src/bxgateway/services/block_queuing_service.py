import time
from abc import ABCMeta, abstractmethod
from collections import deque
from typing import Optional, Dict, Tuple, Deque, TypeVar, Generic, TYPE_CHECKING, List

from bxcommon import constants
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import logger
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

T = TypeVar("T", bound=AbstractMessage)


class BlockQueuingService(Generic[T], metaclass=ABCMeta):
    """
    Service class with following responsibilities:
    1. Make sure that gateway does not send blocks to blockchain node too fast if the previous block has not yet
       been accepted. This condition is ignored if not blocks have been sent yet, or the maximum validation timeout
       has already passed.
    2. Make sure that no new blocks are sent to blockchain node while gateway is waiting for transactions corresponding
       to shorts ids in the previous block if it was not able to find them in local tx service cache
    """

    def __init__(self, node: "AbstractGatewayNode"):

        self.node = node

        # queue of tuple (block hash, timestamp) for blocks that need to be sent to blockchain node
        self._block_queue: Deque[Tuple[Sha256Hash, float]] = deque()

        # dictionary with block hash as key and value of tuple ('waiting for recovery' flag, block message)
        self._blocks: Dict[Sha256Hash, Tuple[bool, T]] = {}

        self._blocks_seen_by_blockchain_node: ExpiringSet[Sha256Hash] = \
            ExpiringSet(node.alarm_queue, gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)

        self._last_block_sent_time: Optional[float] = None
        self._last_alarm_id = None

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
    def get_previous_block_hash_from_message(self, block_message: T) -> Sha256Hash:
        pass

    @abstractmethod
    def on_block_sent(self, block_hash: Sha256Hash, block_message: T):
        pass

    def can_send_block_message(self, block_hash: Sha256Hash, block_message: T) -> bool:
        """
        Determines if a block can be sent.

        The first block on the queue on node startup (whether it gets there from being the first pushed onto the queue
        or because the actually first block failed block recovery) will always get sent.

        Otherwise, wait for previous block hash to be seen to send the block.
        """
        if (not self._block_queue or self._block_queue[0][0] == block_hash) and \
                len(self._blocks_seen_by_blockchain_node) == 0 and self._last_block_sent_time is None:
            return True
        else:
            previous_block_hash = self.get_previous_block_hash_from_message(block_message)
            return previous_block_hash in self._blocks_seen_by_blockchain_node

    def push(self, block_hash: Sha256Hash, block_msg: Optional[T] = None,
             waiting_for_recovery: bool = False):
        """
        Pushes block to the queue
        :param block_hash: Block hash
        :param block_msg: Block message instance (can be None if waiting for recovery flag is set to True
        :param waiting_for_recovery: flag indicating if gateway is waiting for recovery of the block
        """
        if block_msg is None and not waiting_for_recovery:
            raise ValueError("Block message is required if not waiting for recovery of the block.")

        if block_hash in self._blocks:
            raise ValueError("Block with hash {} already exists in the queue.".format(block_hash))

        if block_hash in self._blocks_seen_by_blockchain_node:
            block_stats.add_block_event_by_block_hash(
                block_hash, BlockStatEventType.BLOCK_IGNORE_SEEN_BY_BLOCKCHAIN_NODE, self.node.network_num
            )
            return

        if not waiting_for_recovery and \
                self._is_node_ready_to_accept_blocks() and \
                self.can_send_block_message(block_hash, block_msg):
            self._send_block_to_node(block_hash, block_msg)
            return

        self._block_queue.append((block_hash, time.time()))
        self._blocks[block_hash] = (waiting_for_recovery, block_msg)

        # if it is the first item in the queue then schedule alarm
        if len(self._block_queue) == 1:
            self._schedule_alarm_for_next_item()

    def mark_blocks_seen_by_blockchain_node(self, block_hashes: List[Sha256Hash]):
        """
        Marks blocks seen and retries the top block(s).
        """
        for block_hash in block_hashes:
            self.mark_block_seen_by_blockchain_node(block_hash)
        self._retry_send()

    def mark_block_seen_by_blockchain_node(self, block_hash: Sha256Hash):
        self._blocks_seen_by_blockchain_node.add(block_hash)
        self.remove(block_hash)

    def remove(self, block_hash: Sha256Hash):
        """
        Removes block from the queue if exists
        :param block_hash: block hash
        """
        if block_hash not in self._blocks:
            return

        is_top_item = self._block_queue[0][0] == block_hash

        for index in range(len(self._block_queue)):
            if self._block_queue[index][0] == block_hash:
                del self._block_queue[index]
                del self._blocks[block_hash]

                if is_top_item:
                    self.node.alarm_queue.unregister_alarm(self._last_alarm_id)
                    self._schedule_alarm_for_next_item()
                return

    def update_recovered_block(self, block_hash: Sha256Hash, block_msg: T):
        """
        Updates status of the block in the queue as recovered and ready to send to blockchain node

        :param block_hash: block hash
        :param block_msg: recovered block message
        """
        if block_hash in self._blocks:
            self._blocks[block_hash] = (False, block_msg)

            # if this is the first item in the queue then cancel alarm for block recovery timeout and send block
            if self._block_queue[0][0] == block_hash:
                self.node.alarm_queue.unregister_alarm(self._last_alarm_id)
                self._schedule_alarm_for_next_item()

    def _retry_send(self):
        """
        Checks if top block can be sent to blockchain node, sending if so.
        This method should be called after adding blocks to the set of previous blocks.
        """
        if self._last_alarm_id:
            self.node.alarm_queue.unregister_alarm(self._last_alarm_id)
        self._schedule_alarm_for_next_item()

    def _schedule_alarm_for_next_item(self):
        self._last_alarm_id = None

        if len(self._block_queue) == 0:
            return

        block_hash, timestamp = self._block_queue[0]

        waiting_recovery = self._blocks[block_hash][0]

        if waiting_recovery:
            timeout = constants.MISSING_BLOCK_EXPIRE_TIME - (time.time() - timestamp)
            self._run_or_schedule_alarm(timeout, self._top_block_recovery_timeout)
        else:
            if self.can_send_block_message(block_hash, self._blocks[block_hash][1]):
                timeout = 0
            else:
                timeout = self.node.opts.max_block_interval - (time.time() - self._last_block_sent_time)
            self._run_or_schedule_alarm(timeout, self._send_top_block_to_node)

    def _run_or_schedule_alarm(self, timeout, func):
        if timeout > 0:
            self._last_alarm_id = self.node.alarm_queue.register_alarm(timeout, func)
        elif not self._is_node_ready_to_accept_blocks():
            self.node.alarm_queue.register_alarm(gateway_constants.NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S, func)
        else:
            func()

    def _send_top_block_to_node(self):
        if len(self._block_queue) == 0:
            return

        if not self._is_node_ready_to_accept_blocks():
            self._schedule_alarm_for_next_item()
            return 0

        block_hash = self._block_queue[0][0]
        waiting_recovery, block_msg = self._blocks[block_hash]

        if waiting_recovery:
            logger.warn("Unable to send block to node, requires recovery. Block hash {}.".format(block_hash))
            self._schedule_alarm_for_next_item()
            return

        self._block_queue.popleft()
        del self._blocks[block_hash]

        self._send_block_to_node(block_hash, block_msg)
        self._schedule_alarm_for_next_item()

        return 0

    def _send_block_to_node(self, block_hash: Sha256Hash, block_msg: T):
        self.node.send_msg_to_node(block_msg)

        # if tracking detailed send info, log this event only after all bytes written to sockets
        if not self.node.opts.track_detailed_sent_messages:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE,
                                                      network_num=self._node.network_num,
                                                      more_info="{} bytes. {}".format(len(block_msg.rawbytes(),
                                                                                          block_msg.extra_stats_data())))
        self._last_block_sent_time = time.time()
        self.on_block_sent(block_hash, block_msg)

    def _top_block_recovery_timeout(self) -> int:
        current_time = time.time()

        block_hash, timestamp = self._block_queue[0]
        waiting_block_recovery = self._blocks[block_hash][0]

        if not waiting_block_recovery:
            logger.warn("Unable to cancel recovery for block {}. Block is not in recovery."
                        .format(block_hash))
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        if current_time - timestamp < constants.MISSING_BLOCK_EXPIRE_TIME:
            logger.warn("Unable to cancel recovery for block {}. Block recovery did not timeout."
                        .format(block_hash))
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        self._block_queue.popleft()
        del self._blocks[block_hash]

        self._schedule_alarm_for_next_item()

        return constants.CANCEL_ALARMS

    def _is_node_ready_to_accept_blocks(self) -> bool:
        return self.node.node_conn is not None and self.node.node_conn.is_active()
