import random
import sys
from collections import deque

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import BTC_HDR_COMMON_OFF, BTC_HELLO_MESSAGES
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.messages.btc.addr_btc_message import AddrBTCMessage
from bxcommon.messages.btc.btc_message_factory import btc_message_factory
from bxcommon.messages.btc.btc_message_type import BtcMessageType
from bxcommon.messages.btc.inventory_btc_message import GetDataBTCMessage
from bxcommon.messages.btc.pong_btc_message import PongBTCMessage
from bxcommon.messages.btc.ver_ack_btc_message import VerAckBTCMessage
from bxcommon.messages.btc.version_btc_message import VersionBTCMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages import btc_message_parser


class BTCNodeConnection(GatewayConnection):
    """
    bloXroute gateway <=> blockchain node connection class.

    Attributes
    ----------
    magic: Bitcoin network magic number.
    version_msg: Node connection version message + info.
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
                                        node.opts.blockchain_nonce, 0, node.opts.bloxroute_version,
                                        node.opts.blockchain_services)
        self.enqueue_msg(version_msg)

        self.hello_messages = BTC_HELLO_MESSAGES
        self.header_size = BTC_HDR_COMMON_OFF
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

    def msg_version(self, msg):
        """
        Handle version message.
        Gateway initiates connection, so do not check for misbehavior. Record that we received the version message,
        send a verack, and synchronize chains if need be.
        """
        self.state |= ConnectionState.ESTABLISHED
        reply = VerAckBTCMessage(self.magic)
        self.enqueue_msg(reply)

        if self.state & ConnectionState.ESTABLISHED == ConnectionState.ESTABLISHED:
            for each_msg in self.node.node_msg_queue:
                self.enqueue_msg(each_msg)

            if self.node.node_msg_queue:
                self.node.node_msg_queue = deque()

            self.node.node_conn = self

    def msg_getaddr(self, msg):
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

    def msg_tx(self, msg):
        """
        Handle a TX message by broadcasting to the entire network
        """
        blx_txmsg = btc_message_parser.btc_tx_msg_to_tx_msg(msg)

        # All connections outside of this one is a bloXroute server
        logger.debug("Broadcasting the transaction to peers")
        self.node.broadcast(blx_txmsg, self)
        self.node.tx_service.hash_to_contents[msg.tx_hash()] = msg.tx()

    def msg_block(self, msg):
        """
        Handle a block message. Sends to node for encryption, then broadcasts.
        """
        bloxroute_block = btc_message_parser.btc_block_to_bloxroute_block(msg, self.node.tx_service)
        encrypted_block, block_hash = self.node.in_progress_blocks.encrypt_and_add_payload(bloxroute_block)
        broadcast_message = BroadcastMessage(ObjectHash(block_hash), encrypted_block)
        logger.debug("Compressed block with hash {0} to size {1} from size {2}"
                     .format(block_hash, len(broadcast_message.rawbytes()), len(msg.rawbytes())))
        self.node.block_recovery_service.cancel_recovery_for_block(msg.block_hash())
        self.node.broadcast(broadcast_message, self)

        # TODO: wait for receipt of other messages before sending key
        self.send_key(block_hash)

    def send_key(self, block_hash):
        """
        Sends out the decryption key for a block hash.
        """
        key = self.node.in_progress_blocks.get_encryption_key(block_hash)
        key_message = KeyMessage(ObjectHash(block_hash), key)
        self.node.broadcast(key_message, self)
