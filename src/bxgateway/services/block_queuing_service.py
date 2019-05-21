import time
from collections import deque

from bxcommon import constants
from bxcommon.utils import logger
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway.messages.btc.inventory_btc_message import InventoryType, InvBtcMessage
from bxgateway import gateway_constants


class BlockQueuingService(object):
    """
    Service class with following responsibilities:
    1. Make sure that gateway does not send blocks to blockchain node too fast
       (MIN_INTERVAL_BETWEEN_BLOCKS is minimal interval between blocks)
    2. Make sure that no new blocks are sent to blockchain node while gateway is waiting for transactions corresponding
       to shorts ids in the previous block if it was not able to find them in local tx service cache
    """

    def __init__(self, node):

        self._node = node

        # queue of tuple (block hash, timestamp) for blocks that need to be sent to blockchain node
        self._block_queue = deque()

        # dictionary with block hash as key and value of tuple ('waiting for recovery' flag, block message)
        self._blocks = {}

        self._last_block_sent_time = None
        self._last_alarm_id = None

    def push(self, block_hash, block_msg=None, waiting_for_recovery=False):
        """
        Pushes block to the queue
        :param block_hash: Block hash (ObjectHash instance)
        :param block_msg: Block message instance (can be None if waiting for recovery flag is set to True
        :param waiting_for_recovery: flag indicating if gateway is waiting for recovery of the block
        """
        if not isinstance(block_hash, Sha256Hash):
            raise TypeError("block_hash is expected of type ObjectHash but was {}.".format(type(block_hash)))

        if block_msg is None and not waiting_for_recovery:
            raise ValueError("Block message is required if not waiting for recovery of the block.")

        if block_hash in self._blocks:
            raise ValueError("Block with hash {} already exists in the queue.".format(block_hash))

        # if there is nothing in the queue and enough time since last block then send block to node immediately
        if not waiting_for_recovery and \
                self._is_node_ready_to_accept_blocks() and \
                len(self._block_queue) == 0 and \
                (self._last_block_sent_time is None or (
                        time.time() - self._last_block_sent_time > gateway_constants.MIN_INTERVAL_BETWEEN_BLOCKS_S)):
            self._node.send_msg_to_node(block_msg)
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE,
                                                      network_num=self._node.network_num,
                                                      more_info="{} bytes".format(len(block_msg.rawbytes())))
            self._last_block_sent_time = time.time()
            return

        self._block_queue.append((block_hash, time.time()))
        self._blocks[block_hash] = (waiting_for_recovery, block_msg)

        # if it is the first item in the queue then schedule alarm
        if len(self._block_queue) == 1:
            self._schedule_alarm_for_next_item()

    def remove(self, block_hash):
        """
        Removes block from the queue if exists
        :param block_hash: block hash
        """

        if not isinstance(block_hash, Sha256Hash):
            raise TypeError("block_hash is expected of type ObjectHash but was {}.".format(type(block_hash)))

        if block_hash not in self._blocks:
            return

        is_top_item = self._block_queue[0][0] == block_hash

        for index in range(len(self._block_queue)):
            if self._block_queue[index][0] == block_hash:
                del self._block_queue[index]
                del self._blocks[block_hash]

                if is_top_item:
                    self._node.alarm_queue.unregister_alarm(self._last_alarm_id)
                    self._schedule_alarm_for_next_item()
                return

    def __len__(self):
        """
        Returns number of items in the queue
        :return: number of items in the queue
        """

        return len(self._block_queue)

    def __contains__(self, block_hash):
        """
        Indicates if block hash is in block queue.
        :param block_hash:
        :return: if block hash is in queue
        """
        return block_hash in self._blocks

    def update_recovered_block(self, block_hash, block_msg):
        """
        Updates status of the block in the queue as recovered and ready to send to blockchain node

        :param block_hash: block hash
        :param block_msg: recovered block message
        """

        if not isinstance(block_hash, Sha256Hash):
            raise TypeError("block_hash is expected of type ObjectHash but was {}.".format(type(block_hash)))

        if block_msg is None:
            raise ValueError("block_msg is required")

        if block_hash in self._blocks:
            self._blocks[block_hash] = (False, block_msg)

            # if this is the first item in the queue then cancel alarm for block recovery timeout and send block
            if self._block_queue[0][0] == block_hash:
                self._node.alarm_queue.unregister_alarm(self._last_alarm_id)
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
            timeout = 0 if self._last_block_sent_time is None else gateway_constants.MIN_INTERVAL_BETWEEN_BLOCKS_S - (
                    time.time() - self._last_block_sent_time)
            self._run_or_schedule_alarm(timeout, self._send_top_block_to_node)

    def _run_or_schedule_alarm(self, timeout, func):
        if timeout > 0:
            self._last_alarm_id = self._node.alarm_queue.register_alarm(timeout, func)
        elif not self._is_node_ready_to_accept_blocks():
            self._node.alarm_queue.register_alarm(gateway_constants.NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S, func)
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

        self._node.send_msg_to_node(block_msg)
        self._last_block_sent_time = time.time()
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE,
                                                  network_num=self._node.network_num,
                                                  more_info="{} bytes".format(len(block_msg.rawbytes())))

        self._schedule_alarm_for_next_item()

        return 0

    def _top_block_recovery_timeout(self):
        current_time = time.time()

        block_hash, timestamp = self._block_queue[0]
        waiting_block_recovery = self._blocks[block_hash][0]

        if not waiting_block_recovery:
            logger.warn("Unable to cancel recovery for block {}. Block is not in recovery."
                        .format(block_hash))
            self._schedule_alarm_for_next_item()
            return

        if current_time - timestamp < constants.MISSING_BLOCK_EXPIRE_TIME:
            logger.warn("Unable to cancel recovery for block {}. Block recovery did not timeout."
                        .format(block_hash))
            self._schedule_alarm_for_next_item()
            return

        self._block_queue.popleft()
        del self._blocks[block_hash]

        self._schedule_alarm_for_next_item()

        return 0

    def _is_node_ready_to_accept_blocks(self):
        return self._node.node_conn is not None and self._node.node_conn.is_active()
