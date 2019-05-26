import time
from collections import deque

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants, btc_constants
from bxgateway.btc_constants import NODE_WITNESS_SERVICE_FLAG
from bxgateway.connections.btc.btc_base_connection_protocol import BtcBaseConnectionProtocol
from bxgateway.messages.btc.block_transactions_btc_message import BlockTransactionsBtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.messages.btc.compact_block_decompression import decompress_compact_block, CompactBlockRecoveryItem, \
    decompress_recovered_compact_block
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
            BtcMessageType.GET_DATA: self.msg_getdata,
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

        send_compact_msg = SendCompactBtcMessage(self.magic, on_flag=self.connection.node.opts.compact_block, version=1)

        logger.info("Sending SENDCMPCT message")
        self.connection.node.alarm_queue.register_alarm(2, self.connection.enqueue_msg, send_compact_msg)

        self.connection.node.alarm_queue.register_alarm(gateway_constants.BLOCKCHAIN_PING_INTERVAL_S,
                                                        self.connection.send_ping)

        if self.connection.is_active():
            for each_msg in self.connection.node.node_msg_queue:
                self.connection.enqueue_msg_bytes(each_msg)

            if self.connection.node.node_msg_queue:
                self.connection.node.node_msg_queue = deque()

            self.connection.node.node_conn = self.connection

    def msg_inv(self, msg: InvBtcMessage) -> None:
        """
        Handle an inventory message. Since this is the only node the gateway is connected to,
        assume that everything is new and that we want it.
        :param msg: INV message
        """
        contains_block = False
        inventory_vectors = iter(msg)
        inventory_requests = []
        for inventory_type, item_hash in inventory_vectors:
            if not InventoryType.is_block(inventory_type) or item_hash not in self.connection.node.blocks_seen.contents:
                inventory_requests.append((inventory_type, item_hash))

            if InventoryType.is_block(inventory_type) and item_hash not in self.connection.node.blocks_seen.contents:
                contains_block = True

        if inventory_requests:
            getdata = GetDataBtcMessage(magic=msg.magic(),
                                        inv_vects=inventory_requests,
                                        request_witness_data=self.request_witness_data)
            self.connection.enqueue_msg(getdata, prepend=contains_block)

    def msg_getdata(self, msg: GetDataBtcMessage) -> None:
        """
        Handle GETDATA message from Bitcoin node.
        :param msg: GETDATA message
        """

        for inv_type, object_hash in msg:
            if InventoryType.is_block(inv_type):
                block_stats.add_block_event_by_block_hash(object_hash,
                                                          BlockStatEventType.REMOTE_BLOCK_REQUESTED_BY_GATEWAY,
                                                          network_num=self.connection.network_num,
                                                          more_info="Protocol: {}, Network: {}".format(
                                                              self.connection.node.opts.blockchain_protocol,
                                                              self.connection.node.opts.blockchain_network))
            inv_msg = InvBtcMessage(magic=self.magic, inv_vects=[(InventoryType.MSG_BLOCK, object_hash)])
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
            inv_msg = InvBtcMessage(magic=self.magic, inv_vects=[(InventoryType.MSG_BLOCK, msg.obj_hash())])
            self.connection.node.send_msg_to_node(inv_msg)

    def msg_compact_block(self, msg: CompactBlockBtcMessage) -> None:
        """
        Handle COMPACT BLOCK message from Bitcoin node
        :param msg: COMPACT BLOCK message
        """

        decompression_start_time = time.time()
        parse_result = decompress_compact_block(
            self.magic, msg, self.connection.node.get_tx_service()
        )
        decompression_end_time = time.time()
        logger.info(
            "Compact block {} decompression summary: "
            "duration ({}), "
            "success ({}), "
            "compact short id count ({}), "
            "pre filled transactions count ({})",
            msg.block_hash(),
            decompression_end_time - decompression_start_time,
            parse_result.success,
            parse_result.short_id_count,
            parse_result.pre_filled_tx_count
        )

        if parse_result.success:
            self.msg_block(parse_result.block_btc_message)
        else:
            missing_transactions_indices = parse_result.missing_transactions_indices
            logger.info("Compact block was parsed with {} unknown short ids. Requesting unknown transactions.",
                        len(missing_transactions_indices))
            self._recovery_compact_blocks.add(msg.block_hash(),
                                              CompactBlockRecoveryItem(msg, parse_result.block_transactions,
                                                                       parse_result.missing_transactions_indices))

            get_block_txs_msg = GetBlockTransactionsBtcMessage(magic=self.magic, block_hash=msg.block_hash(),
                                                               indices=parse_result.missing_transactions_indices)
            self.connection.node.send_msg_to_node(get_block_txs_msg)

    def msg_block_transactions(self, msg: BlockTransactionsBtcMessage) -> None:
        """
        Handle BLOCK TRANSACTIONS message from Bitcoin node.
        This is the message that is sent in reply to GET BLOCK TRANSACTIONS message.
        This message exchange happens if gateway is unable to parse compact block from Bitcoin node.
        :param msg: BLOCK TRANSACTIONS message
        """

        if msg.block_hash() in self._recovery_compact_blocks.contents:
            compact_block_recovery_item = self._recovery_compact_blocks.contents[msg.block_hash()]
            assert isinstance(compact_block_recovery_item, CompactBlockRecoveryItem)

            recovery_result = decompress_recovered_compact_block(self.magic,
                                                                 compact_block_recovery_item.compact_block_message,
                                                                 compact_block_recovery_item.block_transactions,
                                                                 compact_block_recovery_item.missing_transactions_indices,
                                                                 msg.transactions())
            if recovery_result.success:
                self.msg_block(recovery_result.block_btc_message)
            else:
                logger.info(
                    "Unable to recover compact block '{}' "
                    "after receiving BLOCK TRANSACTIONS message. Requesting full block.",
                    msg.block_hash()
                )
                get_data_msg = GetDataBtcMessage(magic=self.magic,
                                                 inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())])
                self.connection.node.send_msg_to_node(get_data_msg)
