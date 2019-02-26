import datetime
import time

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.utils import crypto, convert, logger
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxgateway.messages.gateway.block_holding_message import BlockHoldingMessage


class BlockHold(object):
    """
    Data class for holds on block messages.

    Attributes
    ----------
    hold_message_time: time the hold message was received. Currently unused, for use later to determine if Gateway is
                       sending malicious hold messages
    holding_connection: connection that sent the original hold message
    block_message: message being delayed due to an existing hold
    alarm: reference to alarm object for hold timeout
    held_connection: connection from which the message is being delayed for
    """

    def __init__(self, hold_message_time, holding_connection):
        self.hold_message_time = hold_message_time
        self.holding_connection = holding_connection

        self.block_message = None
        self.alarm = None
        self.held_connection = None


class BlockProcessingService(object):
    """
    Service class that process blocks.
    Blocks received from blockchain node are held if gateway receives a `blockhold` message from another gateway to
    prevent duplicate message sending.
    """

    def __init__(self, node):
        self._node = node
        self._holds = ExpiringDict(node.alarm_queue, gateway_constants.BLOCK_HOLD_DURATION_S)

    def place_hold(self, block_hash, connection):
        """
        Places hold on block hash and propagates message.
        :param block_hash: ObjectHash
        :param connection:
        """
        if block_hash in self._node.blocks_seen.contents:
            return

        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_REQUESTED,
                                                  network_num=connection.network_num,
                                                  peer=connection.peer_desc)

        if block_hash not in self._holds.contents:
            self._holds.add(block_hash, BlockHold(time.time(), connection))
            self._node.broadcast(BlockHoldingMessage(block_hash), broadcasting_conn=connection,
                                 connection_type=ConnectionType.GATEWAY)

    def queue_block_for_processing(self, block_message, connection):
        """
        Queues up block for processing on timeout if hold message received.
        If no hold exists, compress and broadcast block immediately.
        :param block_message: block message to process
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        if block_hash in self._holds.contents:
            hold = self._holds.contents[block_hash]
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_HELD_BLOCK,
                                                      timeout_already_scheduled=hold.alarm is not None,
                                                      network_num=connection.network_num,
                                                      peer=connection.peer_desc)

            if hold.alarm is None:
                hold.alarm = self._node.alarm_queue.register_alarm(gateway_constants.BLOCK_HOLDING_TIMEOUT_S,
                                                                   self._holding_timeout, block_hash, hold)
                hold.block_message = block_message
                hold.connection = connection
        else:
            self._node.broadcast(BlockHoldingMessage(block_hash), broadcasting_conn=connection,
                                 connection_type=ConnectionType.GATEWAY, prepend_to_queue=True)
            self._process_and_broadcast_block(block_message, connection)

    def cancel_hold_timeout(self, block_hash, connection):
        """
        Lifts hold on block hash and cancels timeout.
        :param block_hash: ObjectHash
        :param connection: connection cancelling hold
        """
        if block_hash in self._holds.contents:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_LIFTED,
                                                      network_num=connection.network_num,
                                                      peer=connection.peer_desc)

            hold = self._holds.contents[block_hash]
            if hold.alarm is not None:
                self._node.alarm_queue.unregister_alarm(hold.alarm)
            del self._holds.contents[block_hash]

    def _compute_hold_timeout(self, block_message):
        """
        Computes timeout after receiving block message before sending the block anyway if not received from network.
        TODO: implement algorithm for computing hold timeout
        :param block_message: block message to hold
        :return: time in seconds to wait
        """
        return gateway_constants.BLOCK_HOLDING_TIMEOUT_S

    def _holding_timeout(self, block_hash, hold):
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HOLD_TIMED_OUT,
                                                  network_num=hold.connection.network_num,
                                                  peer=hold.connection.peer_desc)
        self._process_and_broadcast_block(hold.block_message, hold.connection)

    def _process_and_broadcast_block(self, block_message, connection):
        """
        Compresses and propagates block message.
        :param block_message: block message to propagate
        :param connection: receiving connection (AbstractBlockchainConnection)
        """
        block_hash = block_message.block_hash()
        compress_start = datetime.datetime.utcnow()
        bx_block, block_info = connection.message_converter.block_to_bx_block(block_message,
                                                                              self._node.get_tx_service())
        compress_end = datetime.datetime.utcnow()

        bx_block_hash = crypto.double_sha256(bx_block)
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.BLOCK_COMPRESSED,
                                                  start_date_time=compress_start,
                                                  end_date_time=compress_end,
                                                  network_num=connection.network_num,
                                                  original_size=len(block_message.rawbytes()),
                                                  compressed_size=len(bx_block),
                                                  compressed_hash=convert.bytes_to_hex(bx_block_hash),
                                                  txs_count=block_info[0],
                                                  prev_block_hash=block_info[1])

        connection.node.neutrality_service.propagate_block_to_network(bx_block, connection, block_hash)

        connection.node.block_recovery_service.cancel_recovery_for_block(block_hash)
        connection.node.block_queuing_service.remove(block_hash)
        connection.node.blocks_seen.add(block_hash)
        connection.node.get_tx_service().track_seen_short_ids(block_info[2])
