from collections import deque

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxgateway.btc_constants import NODE_WITNESS_SERVICE_FLAG
from bxgateway.connections.btc.btc_base_connection_protocol import BtcBaseConnectionProtocol
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InventoryType
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
        })

        self.request_witness_data = False

    def msg_version(self, msg):
        """
        Handle version message.
        Gateway initiates connection, so do not check for misbehavior. Record that we received the version message,
        send a verack, and synchronize chains if need be.
        """
        if not isinstance(msg, VersionBtcMessage):
            raise ValueError("Message of type VersionBtcMessage is expected but was {}".format(type(msg)))

        self.request_witness_data = msg.services() & NODE_WITNESS_SERVICE_FLAG > 0

        if self.request_witness_data:
            logger.info("Detected connection with BTC node supporting SegWit.")

        self.connection.state |= ConnectionState.ESTABLISHED
        reply = VerAckBtcMessage(self.magic)
        self.connection.enqueue_msg(reply)
        self.connection.node.alarm_queue.register_alarm(gateway_constants.BLOCKCHAIN_PING_INTERVAL_S,
                                                        self.connection.send_ping)

        if self.connection.is_active():
            for each_msg in self.connection.node.node_msg_queue:
                self.connection.enqueue_msg_bytes(each_msg)

            if self.connection.node.node_msg_queue:
                self.connection.node.node_msg_queue = deque()

            self.connection.node.node_conn = self.connection

    def msg_inv(self, msg):
        """
        Handle an inventory message. Since this is the only node the gateway is connected to,
        assume that everything is new and that we want it.
        """
        inventory_vectors = iter(msg)
        inventory_requests = []
        for inventory_type, item_hash in inventory_vectors:
            if not InventoryType.is_block(inventory_type) or item_hash not in self.connection.node.blocks_seen.contents:
                inventory_requests.append((inventory_type, item_hash))
        if inventory_requests:
            getdata = GetDataBtcMessage(magic=msg.magic(),
                                        inv_vects=inventory_requests,
                                        request_witness_data=self.request_witness_data)
            self.connection.enqueue_msg(getdata)

    def msg_getdata(self, msg):
        # type: (GetDataBtcMessage) -> None
        for inv_type, object_hash in msg:
            if InventoryType.is_block(inv_type):
                block_stats.add_block_event_by_block_hash(object_hash,
                                                          BlockStatEventType.REMOTE_BLOCK_REQUESTED_BY_GATEWAY,
                                                          network_num=self.connection.network_num,
                                                          blockchain_protocol=self.connection.node.opts.blockchain_protocol,
                                                          blockchain_network=self.connection.node.opts.blockchain_network)

        return self.msg_proxy_request(msg)
