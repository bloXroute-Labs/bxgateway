import time
from abc import abstractmethod
from typing import Optional, TypeVar, TYPE_CHECKING, List, Generic

from bxutils import logging

from bxcommon import constants
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)

TMessage = TypeVar("TMessage", bound=AbstractMessage)


class BlockQueuingService(Generic[TMessage], AbstractBlockQueuingService[TMessage]):
    """
    Service class with following responsibilities:
    1. Make sure that gateway does not send blocks to blockchain node too fast if the previous block has not yet
       been accepted. This condition is ignored if not blocks have been sent yet, or the maximum validation timeout
       has already passed.
    2. Make sure that no new blocks are sent to blockchain node while gateway is waiting for transactions corresponding
       to shorts ids in the previous block if it was not able to find them in local tx service cache
    """

    def __init__(self, node: "AbstractGatewayNode"):

        super(BlockQueuingService, self).__init__(node=node)

        self._last_block_sent_time: float = 0.0
        self._last_alarm_id = None

    @abstractmethod
    def get_previous_block_hash_from_message(self, block_message: TMessage) -> Sha256Hash:
        pass

    @abstractmethod
    def on_block_sent(self, block_hash: Sha256Hash, block_message: TMessage):
        pass

    def push(self, block_hash: Sha256Hash, block_msg: Optional[TMessage] = None, waiting_for_recovery: bool = False):
        if not self.can_add_block_to_queuing_service(block_hash, block_msg, waiting_for_recovery):
            return

        if not waiting_for_recovery and \
                self._is_node_ready_to_accept_blocks() and \
                self._can_send_block_message(block_hash, block_msg):
            self._send_block_to_node(block_hash, block_msg)
            return

        logger.debug("Queuing up block {} for sending to the blockchain node. Block is behind {} others.",
                     block_hash, len(self._block_queue))

        self._block_queue.append((block_hash, time.time()))
        self._blocks[block_hash] = (waiting_for_recovery, block_msg)

        # if it is the first item in the queue then schedule alarm
        if len(self._block_queue) == 1:
            self._schedule_alarm_for_next_item()

    def remove(self, block_hash: Sha256Hash):
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

    def update_recovered_block(self, block_hash: Sha256Hash, block_msg: TMessage):
        if block_hash in self._blocks:
            self._blocks[block_hash] = (False, block_msg)

            # if this is the first item in the queue then cancel alarm for block recovery timeout and send block
            if self._block_queue[0][0] == block_hash:
                self.node.alarm_queue.unregister_alarm(self._last_alarm_id)
                self._schedule_alarm_for_next_item()

    def _send_block_to_node(self, block_hash: Sha256Hash, block_msg: TMessage):
        super(BlockQueuingService, self)._send_block_to_node(block_hash, block_msg)
        self._last_block_sent_time = time.time()
        self.on_block_sent(block_hash, block_msg)

    def _can_send_block_message(self, block_hash: Sha256Hash, block_message: TMessage) -> bool:
        """
        Determines if a block can be sent.

        The first block on the queue on node startup (whether it gets there from being the first pushed onto the queue
        or because the actually first block failed block recovery) will always get sent.

        Otherwise, wait for previous block hash to be seen to send the block.
        """
        if (not self._block_queue or self._block_queue[0][0] == block_hash) and \
                len(self._blocks_seen_by_blockchain_node) == 0 and self._last_block_sent_time == 0.0:
            return True
        else:
            previous_block_hash = self.get_previous_block_hash_from_message(block_message)
            return previous_block_hash in self._blocks_seen_by_blockchain_node

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
            timeout = gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME - (time.time() - timestamp)
            self._run_or_schedule_alarm(timeout, self._top_block_recovery_timeout)
        else:
            if self._can_send_block_message(block_hash, self._blocks[block_hash][1]):
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
            logger.debug("Unable to send block to node, requires recovery. Block hash {}.", block_hash)
            self._schedule_alarm_for_next_item()
            return

        self._block_queue.popleft()
        del self._blocks[block_hash]

        self._send_block_to_node(block_hash, block_msg)
        self._schedule_alarm_for_next_item()

        return 0

    def _top_block_recovery_timeout(self) -> int:
        current_time = time.time()

        block_hash, timestamp = self._block_queue[0]
        waiting_block_recovery = self._blocks[block_hash][0]

        if not waiting_block_recovery:
            logger.debug("Unable to cancel recovery for block {}. Block is not in recovery.", block_hash)
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        if current_time - timestamp < gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME:
            logger.debug("Unable to cancel recovery for block {}. Block recovery did not timeout.", block_hash)
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        self._block_queue.popleft()
        del self._blocks[block_hash]

        self._schedule_alarm_for_next_item()

        return constants.CANCEL_ALARMS

    def _is_node_ready_to_accept_blocks(self) -> bool:
        return self.node.node_conn is not None and self.node.node_conn.is_active()
