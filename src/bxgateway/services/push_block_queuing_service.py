import time
from abc import abstractmethod, ABCMeta
from typing import Optional, TYPE_CHECKING, List, Generic, Callable, cast

from bxcommon import constants
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.services.abstract_block_queuing_service import (
    AbstractBlockQueuingService,
    TBlockMessage,
    THeaderMessage,
)
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class PushBlockQueuingService(
    AbstractBlockQueuingService[TBlockMessage, THeaderMessage],
    Generic[TBlockMessage, THeaderMessage],
    metaclass=ABCMeta,
):
    """
    Service class with following responsibilities:

    1. Make sure that gateway does not send blocks to blockchain node too fast
       if the previous block has not yet been accepted. This condition is
       ignored if not blocks have been sent yet, or the maximum validation
       timeout has already passed.

    2. Make sure that no new blocks are sent to blockchain node while gateway
       is waiting for transactions corresponding to shorts ids in the previous
       block if it was not able to find them in local tx service cache

    This implementation actively pushes new blocks to the blockchain node
    (as opposed to a "pull-based" approach that sends announcements and lets
    blockchain nodes request them).
    """

    def __init__(
        self,
        node: "AbstractGatewayNode",
        connection: AbstractGatewayBlockchainConnection
    ):
        super(PushBlockQueuingService, self).__init__(node, connection)

        self._last_block_sent_time: float = 0.0
        self._last_alarm_id: Optional[AlarmId] = None

    @abstractmethod
    def get_previous_block_hash_from_message(
        self, block_message: TBlockMessage
    ) -> Sha256Hash:
        pass

    @abstractmethod
    def on_block_sent(
        self, block_hash: Sha256Hash, block_message: TBlockMessage
    ):
        pass

    def push(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[TBlockMessage] = None,
        waiting_for_recovery: bool = False,
    ) -> None:
        super().push(block_hash, block_msg, waiting_for_recovery)

        if (
            not waiting_for_recovery and
            self.node.has_active_blockchain_peer()
        ):
            assert block_msg is not None
            if self._can_send_block_message(block_hash, block_msg):
                self.send_block_to_node(block_hash, block_msg)
                self.remove_from_queue(block_hash)
                return

        logger.debug(
            "Queued up block {} for sending to the blockchain node {}. "
            "Block is behind {} others.",
            block_hash,
            self.connection.peer_desc,
            len(self._block_queue) - 1,
        )

        # if it is the first item in the queue then schedule alarm
        if len(self._block_queue) == 1:
            self._schedule_alarm_for_next_item()

    def update_recovered_block(
        self, block_hash: Sha256Hash, block_msg: TBlockMessage
    ) -> None:
        if block_hash in self._blocks:
            self._blocks_waiting_for_recovery[block_hash] = False

            # if this is the first item in the queue then cancel alarm for
            # block recovery timeout and send block
            if len(self._block_queue) > 0 and self._block_queue[0][0] == block_hash:
                last_alarm_id = self._last_alarm_id
                assert last_alarm_id is not None
                self.node.alarm_queue.unregister_alarm(last_alarm_id)
                self._schedule_alarm_for_next_item()

    def mark_blocks_seen_by_blockchain_node(
        self, block_hashes: List[Sha256Hash]
    ) -> None:
        """
        Marks blocks seen and retries the top block(s).
        """
        for block_hash in block_hashes:
            self.mark_block_seen_by_blockchain_node(block_hash, None)
        self._retry_send()

    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        block_message: Optional[TBlockMessage],
        block_number: Optional[int] = None
    ) -> None:
        super().mark_block_seen_by_blockchain_node(block_hash, block_message)
        self._blocks_seen_by_blockchain_node.add(block_hash)
        self.remove_from_queue(block_hash)

    def send_block_to_node(
        self, block_hash: Sha256Hash, block_msg: Optional[TBlockMessage] = None
    ) -> None:
        if block_msg is None:
            block_msg = self.node.block_storage[block_hash]

        super(PushBlockQueuingService, self).send_block_to_node(
            block_hash, block_msg
        )
        self._last_block_sent_time = time.time()
        # pyre-fixme[6]: Expected `TBlockMessage` for 2nd param but got
        #  `Optional[Variable[TBlockMessage (bound to
        #  bxcommon.messages.abstract_message.AbstractMessage)]]`.
        self.on_block_sent(block_hash, block_msg)

    def remove_from_queue(self, block_hash: Sha256Hash) -> int:
        index = super().remove_from_queue(block_hash)

        if index == 0 and self._last_alarm_id is not None:
            # pyre-fixme[6]: Expected `AlarmId` for 1st param but got
            #  `Optional[AlarmId]`.
            self.node.alarm_queue.unregister_alarm(self._last_alarm_id)
            self._schedule_alarm_for_next_item()

        return index

    def _can_send_block_message(
        self, block_hash: Sha256Hash, block_message: TBlockMessage
    ) -> bool:
        """
        Determines if a block can be sent.

        The first block on the queue on node startup (whether it gets there from
        being the first pushed onto the queue or because the actually first
        block failed block recovery) will always get sent.

        Otherwise, wait for previous block hash to be seen to send the block.
        """
        if (
            (not self._block_queue or self._block_queue[0][0] == block_hash)
            and len(self._blocks_seen_by_blockchain_node) == 0
            and self._last_block_sent_time == 0.0
        ):
            return True
        else:
            previous_block_hash = self.get_previous_block_hash_from_message(
                block_message
            )
            return previous_block_hash in self._blocks_seen_by_blockchain_node

    def _retry_send(self) -> None:
        """
        Checks if top block can be sent to blockchain node, sending if so.
        This method should be called after adding blocks to the
        set of previous blocks.
        """
        last_alarm_id = self._last_alarm_id
        if last_alarm_id:
            self.node.alarm_queue.unregister_alarm(last_alarm_id)
        self._schedule_alarm_for_next_item()

    def _schedule_alarm_for_next_item(self) -> None:
        self._last_alarm_id = None

        if len(self._block_queue) == 0:
            return

        block_hash, timestamp = self._block_queue[0]
        waiting_recovery = self._blocks_waiting_for_recovery[block_hash]

        if waiting_recovery:
            timeout = gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME - (
                time.time() - timestamp
            )
            self._run_or_schedule_alarm(
                timeout, self._top_block_recovery_timeout
            )
        else:
            if not self._is_elapsed_time_shorter_than_ttl(time.time() - timestamp, block_hash):
                self._remove_old_blocks_from_queue()
                self._schedule_alarm_for_next_item()
                return
            block_message = cast(TBlockMessage, self.node.block_storage[block_hash])
            assert block_message is not None
            if self._can_send_block_message(block_hash, block_message):
                timeout = 0
            else:
                timeout = self.node.opts.max_block_interval_s - (
                    time.time() - self._last_block_sent_time
                )
            self._run_or_schedule_alarm(timeout, self._send_top_block_to_node)

    def _run_or_schedule_alarm(self, timeout: float, func: Callable) -> None:
        if timeout > 0:
            self._last_alarm_id = self.node.alarm_queue.register_alarm(
                timeout, func
            )
        elif not self.node.has_active_blockchain_peer():
            self.node.alarm_queue.register_alarm(
                gateway_constants.NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S,
                func,
            )
        else:
            func()

    def _send_top_block_to_node(self) -> int:
        if len(self._block_queue) == 0:
            return constants.CANCEL_ALARMS

        if not self.node.has_active_blockchain_peer():
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        block_hash, timestamp = self._block_queue[0]
        waiting_recovery = self._blocks_waiting_for_recovery[block_hash]

        if waiting_recovery:
            logger.debug(
                "Unable to send block to node {}, requires recovery. "
                "Block hash {}.",
                self.connection.peer_desc,
                block_hash,
            )
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        block_msg = cast(TBlockMessage, self.node.block_storage[block_hash])
        index = self.remove_from_queue(block_hash)
        assert index == 0

        if self._is_elapsed_time_shorter_than_ttl(time.time() - timestamp, block_hash):
            self.send_block_to_node(block_hash, block_msg)
        else:
            self._remove_old_blocks_from_queue()

        self._schedule_alarm_for_next_item()
        return constants.CANCEL_ALARMS

    def _top_block_recovery_timeout(self) -> int:
        if len(self._block_queue) == 0:
            return constants.CANCEL_ALARMS

        current_time = time.time()

        block_hash, timestamp = self._block_queue[0]
        waiting_block_recovery = self._blocks_waiting_for_recovery[block_hash]

        if not waiting_block_recovery:
            logger.debug(
                "Unable to cancel recovery for block {} in queuing service for {}. "
                "Block is not in recovery.",
                block_hash,
                self.connection.peer_desc
            )
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        if (
            current_time - timestamp
            < gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME
        ):
            logger.debug(
                "Unable to cancel recovery for block {} in queuing service for {}. "
                "Block recovery did not timeout.",
                block_hash,
                self.connection.peer_desc
            )
            self._schedule_alarm_for_next_item()
            return constants.CANCEL_ALARMS

        index = self.remove(block_hash)
        assert index == 0

        self._schedule_alarm_for_next_item()

        return constants.CANCEL_ALARMS

    def _is_elapsed_time_shorter_than_ttl(self, elapsed_time: float, block_hash: Sha256Hash) -> bool:
        if elapsed_time < self.node.opts.blockchain_message_ttl:
            return True

        logger.debug("Skipping block {}, since {:.2f}s have passed since receiving the block.",
                     block_hash, elapsed_time)
        return False

    def _remove_old_blocks_from_queue(self) -> None:
        """
        Removes old blocks from block queue even if block_hash is not in self._blocks, similar to remove_from_queue
        """
        logger.trace("Checking block queue for {} and removing old blocks from queue.", self.connection.peer_desc)
        current_time = time.time()
        block_queue_start_len = len(self._block_queue)
        while len(self._block_queue) > 0:
            block_hash, timestamp = self._block_queue[0]
            if current_time - timestamp < self.node.opts.blockchain_message_ttl:
                return

            last_alarm_id = self._last_alarm_id
            if block_queue_start_len == len(self._block_queue) and last_alarm_id is not None:
                self.node.alarm_queue.unregister_alarm(last_alarm_id)
                self._schedule_alarm_for_next_item()

            del self._block_queue[0]
            del self._blocks_waiting_for_recovery[block_hash]
