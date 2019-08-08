import time
from collections import deque

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger, convert
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants, btc_constants
from bxgateway.btc_constants import NODE_WITNESS_SERVICE_FLAG
from bxgateway.connections.btc.btc_base_connection_protocol import BtcBaseConnectionProtocol
from bxgateway.messages.btc.block_transactions_btc_message import BlockTransactionsBtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.messages.btc.get_block_transactions_btc_message import GetBlockTransactionsBtcMessage
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InventoryType, InvBtcMessage
from bxgateway.messages.btc.send_compact_btc_message import SendCompactBtcMessage
from bxgateway.messages.btc.ver_ack_btc_message import VerAckBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage


class BtcNodeConnectionProtocol(BtcBaseConnectionProtocol):
    def __init__(self, connection):
        super(BtcNodeConnectionProtocol, self).__init__(connection)

        connection.message_handlers.update({
            BtcMessageType.VERSION: self.msg_version,
            BtcMessageType.INVENTORY: self.msg_inv,
            BtcMessageType.BLOCK: self.msg_block,
            BtcMessageType.TRANSACTIONS: self.msg_tx,
            BtcMessageType.GET_BLOCKS: self.msg_proxy_request,
            BtcMessageType.GET_HEADERS: self.msg_proxy_request,
            BtcMessageType.GET_DATA: self.msg_get_data,
            BtcMessageType.REJECT: self.msg_reject,
            BtcMessageType.COMPACT_BLOCK: self.msg_compact_block,
            BtcMessageType.BLOCK_TRANSACTIONS: self.msg_block_transactions
        })

        self.request_witness_data = False
        self._recovery_compact_blocks = ExpiringDict(self.connection.node.alarm_queue,
                                                     btc_constants.BTC_COMPACT_BLOCK_RECOVERY_TIMEOUT_S)

    def msg_version(self, msg: VersionBtcMessage) -> None:
        """
        Handle version message.
        Gateway initiates connection, so do not check for misbehavior. Record that we received the version message,
        send a verack, and synchronize chains if need be.
        :param msg: VERSION message
        """
        self.request_witness_data = msg.services() & NODE_WITNESS_SERVICE_FLAG > 0

        if self.request_witness_data:
            logger.info("Detected connection with BTC node supporting SegWit.")

        self.connection.state |= ConnectionState.ESTABLISHED
        reply = VerAckBtcMessage(self.magic)
        self.connection.enqueue_msg(reply)

        send_compact_msg = SendCompactBtcMessage(
            self.magic, on_flag=self.connection.node.opts.compact_block, version=1
        )

        logger.info("Sending SENDCMPCT message")
        self.connection.node.alarm_queue.register_alarm(
            2, self.connection.enqueue_msg, send_compact_msg
        )

        self.connection.node.alarm_queue.register_alarm(
            gateway_constants.BLOCKCHAIN_PING_INTERVAL_S,
            self.connection.send_ping
        )

        if self.connection.is_active():
            for each_msg in self.connection.node.node_msg_queue:
                self.connection.enqueue_msg_bytes(each_msg)

            if self.connection.node.node_msg_queue:
                self.connection.node.node_msg_queue = deque()

            self.connection.node.node_conn = self.connection

    def msg_inv(self, msg: InvBtcMessage) -> None:
        """
        Handle an inventory message.

        Requests all transactions and blocks that haven't been previously seen.
        :param msg: INV message
        """
        contains_block = False
        inventory_requests = []
        block_hashes = []
        for inventory_type, item_hash in msg:
            if not InventoryType.is_block(inventory_type) or \
                    item_hash not in self.connection.node.blocks_seen.contents:
                inventory_requests.append((inventory_type, item_hash))

            if InventoryType.is_block(inventory_type):
                if item_hash not in self.connection.node.blocks_seen.contents:
                    contains_block = True
                block_hashes.append(item_hash)

        if inventory_requests:
            get_data = GetDataBtcMessage(
                magic=msg.magic(),
                inv_vects=inventory_requests,
                request_witness_data=self.request_witness_data
            )
            self.connection.enqueue_msg(get_data, prepend=contains_block)

        self.connection.node.block_queuing_service.mark_blocks_seen_by_blockchain_node(block_hashes)

    def msg_get_data(self, msg: GetDataBtcMessage) -> None:
        """
        Handle GETDATA message from Bitcoin node.
        :param msg: GETDATA message
        """

        for inv_type, object_hash in msg:
            if InventoryType.is_block(inv_type):
                block_stats.add_block_event_by_block_hash(
                    object_hash,
                    BlockStatEventType.REMOTE_BLOCK_REQUESTED_BY_GATEWAY,
                    network_num=self.connection.network_num,
                    more_info="Protocol: {}, Network: {}".format(
                        self.connection.node.opts.blockchain_protocol,
                        self.connection.node.opts.blockchain_network
                    )
                )
            inv_msg = InvBtcMessage(
                magic=self.magic, inv_vects=[(InventoryType.MSG_BLOCK, object_hash)]
            )
            self.connection.node.send_msg_to_node(inv_msg)
        return self.msg_proxy_request(msg)

    def msg_reject(self, msg):
        """
        Handle REJECT message from Bitcoin node
        :param msg: REJECT message
        """

        # Send inv message to the send in case of rejected block
        # remaining sync communication will proxy to remote blockchain node
        if msg.message() == BtcMessageType.BLOCK:
            inv_msg = InvBtcMessage(
                magic=self.magic, inv_vects=[(InventoryType.MSG_BLOCK, msg.obj_hash())]
            )
            self.connection.node.send_msg_to_node(inv_msg)

    def msg_compact_block(self, msg: CompactBlockBtcMessage) -> None:
        """
        Handle COMPACT BLOCK message from Bitcoin node
        :param msg: COMPACT BLOCK message
        """

        block_hash = msg.block_hash()
        short_ids_count = len(msg.short_ids())
        block_stats.add_block_event_by_block_hash(
            block_hash,
            BlockStatEventType.COMPACT_BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE,
            network_num=self.connection.network_num,
            peer=self.connection.peer_desc,
            more_info="{} short ids".format(short_ids_count)
        )

        if block_hash in self.connection.node.blocks_seen.contents:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.COMPACT_BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                network_num=self.connection.network_num,
                peer=self.connection.peer_desc
            )
            logger.debug("Have seen block {0} before. Ignoring.".format(block_hash))
            return

        max_time_offset = self.connection.node.opts.blockchain_block_interval * self.connection.node.opts.blockchain_ignore_block_interval_count
        if time.time() - msg.timestamp() >= max_time_offset:
            logger.debug(
                "Received block {} more than {} seconds after it was created ({}). "
                "Ignoring.".format(block_hash, max_time_offset, msg.timestamp()))
            return

        if short_ids_count < self.connection.node.opts.compact_block_min_tx_count:
            logger.info(
                "Compact block {} contains {} short transactions, less than limit {}. Requesting full block.",
                convert.bytes_to_hex(msg.block_hash().binary),
                short_ids_count,
                btc_constants.BTC_COMPACT_BLOCK_DECOMPRESS_MIN_TX_COUNT
            )
            get_data_msg = GetDataBtcMessage(
                magic=self.magic,
                inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())]
            )
            self.connection.node.send_msg_to_node(get_data_msg)
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.COMPACT_BLOCK_REQUEST_FULL,
                                                      network_num=self.connection.network_num)
            return

        self.connection.node.on_block_seen_by_blockchain_node(block_hash)
        # pyre-ignore
        parse_result = self.connection.node.block_processing_service.process_compact_block(msg, self.connection)
        if not parse_result.success:
            self._recovery_compact_blocks.add(block_hash, parse_result)

            get_block_txs_msg = GetBlockTransactionsBtcMessage(magic=self.magic, block_hash=block_hash,
                                                               indices=parse_result.missing_indices)
            self.connection.node.send_msg_to_node(get_block_txs_msg)

    def msg_block_transactions(self, msg: BlockTransactionsBtcMessage) -> None:
        """
        Handle BLOCK TRANSACTIONS message from Bitcoin node.
        This is the message that is sent in reply to GET BLOCK TRANSACTIONS message.
        This message exchange happens if gateway is unable to parse compact block from Bitcoin node.
        :param msg: BLOCK TRANSACTIONS message
        """

        if msg.block_hash() in self._recovery_compact_blocks.contents:
            recovery_result = self._recovery_compact_blocks.contents[msg.block_hash()]
            self.connection.node.block_processing_service.process_compact_block_recovery(  # pyre-ignore
                msg, recovery_result, self.connection
            )
