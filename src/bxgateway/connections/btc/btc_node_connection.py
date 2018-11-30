import random
import sys
from collections import deque

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.btc.addr_btc_message import AddrBTCMessage
from bxcommon.messages.btc.btc_message_factory import btc_message_factory
from bxcommon.messages.btc.btc_message_type import BtcMessageType
from bxcommon.messages.btc.inventory_btc_message import GetDataBTCMessage
from bxcommon.messages.btc.ping_btc_message import PingBTCMessage
from bxcommon.messages.btc.pong_btc_message import PongBTCMessage
from bxcommon.messages.btc.ver_ack_btc_message import VerAckBTCMessage
from bxcommon.messages.btc.version_btc_message import VersionBTCMessage
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.messages.btc.btc_message_converter import BtcMessageConverter


class BTCNodeConnection(AbstractGatewayBlockchainConnection):
    """
    bloXroute gateway <=> blockchain node connection class.

    Attributes
    ----------
    magic: Bitcoin network magic number.
    message_handlers: Overridden from AbstractConnection. Maps message types to method handlers.
    """
    ESTABLISHED = 0b1
    NONCE = random.randint(0, sys.maxint)  # Used to detect connections to self.

    connection_type = ConnectionType.BLOCKCHAIN_NODE

    def __init__(self, sock, address, node, from_me=False):
        super(BTCNodeConnection, self).__init__(sock, address, node)

        self.magic = node.opts.blockchain_net_magic

        # Establish connection with blockchain node
        version_msg = VersionBTCMessage(node.opts.blockchain_net_magic, node.opts.blockchain_version, self.peer_ip,
                                        self.peer_port, self.node.opts.external_ip, self.node.opts.external_port,
                                        node.opts.blockchain_nonce, 0, str(node.opts.protocol_version),
                                        node.opts.blockchain_services)
        self.enqueue_msg(version_msg)

        self.hello_messages = constants.BTC_HELLO_MESSAGES
        self.header_size = constants.BTC_HDR_COMMON_OFF
        self.message_factory = btc_message_factory
        self.message_handlers = {
            BtcMessageType.PING: self.msg_ping,
            BtcMessageType.PONG: self.msg_pong,
            BtcMessageType.VERSION: self.msg_version,
            BtcMessageType.BLOCK: self.msg_block,
            BtcMessageType.TRANSACTIONS: self.msg_tx,
            BtcMessageType.GET_ADDRESS: self.msg_getaddr,
            BtcMessageType.INVENTORY: self.msg_inv,
        }
        self.ping_btc_message = PingBTCMessage(self.magic)

        self.message_converter = BtcMessageConverter(self.magic)

    ###
    # Handlers for each message type
    ###

    def msg_ping(self, msg):
        """
        Handle ping messages. Respond with a pong.
        """
        reply = PongBTCMessage(self.magic, msg.nonce())
        self.enqueue_msg(reply)

    def msg_pong(self, msg):
        """
        Handle pong messages. For now, don't do anything since we don't ping.
        """
        pass

    def msg_version(self, _msg):
        """
        Handle version message.
        Gateway initiates connection, so do not check for misbehavior. Record that we received the version message,
        send a verack, and synchronize chains if need be.
        """
        self.state |= ConnectionState.ESTABLISHED
        reply = VerAckBTCMessage(self.magic)
        self.enqueue_msg(reply)
        self.node.alarm_queue.register_alarm(constants.BLOCKCHAIN_PING_INTERVAL_S, self.send_ping)

        if self.state & ConnectionState.ESTABLISHED == ConnectionState.ESTABLISHED:
            for each_msg in self.node.node_msg_queue:
                self.enqueue_msg_bytes(each_msg)

            if self.node.node_msg_queue:
                self.node.node_msg_queue = deque()

            self.node.node_conn = self

    def msg_getaddr(self, _msg):
        """
        Handle a getaddr message. Return a blank address to preserve privacy.
        """
        reply = AddrBTCMessage(self.magic)
        self.enqueue_msg(reply)

    def msg_inv(self, msg):
        """
        Handle an inventory message. Since this is the only node the gateway is connected to,
        assume that everything is new and that we want it.
        """
        getdata = GetDataBTCMessage(magic=msg.magic(), inv_vects=[x for x in msg])
        self.enqueue_msg(getdata)

    def send_ping(self):
        """
        Sends ping. Overrides with blockchain message type.
        """
        self.enqueue_msg(self.ping_btc_message)
        return constants.BLOCKCHAIN_PING_INTERVAL_S