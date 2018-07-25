import random
import sys
from collections import deque

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import btc_magic_numbers, HDR_COMMON_OFF, BTC_HDR_COMMON_OFF
from bxcommon.messages.btc.addr_btc_message import AddrBTCMessage
from bxcommon.messages.btc.btc_message import BTCMessage
from bxcommon.messages.btc.inventory_btc_message import GetDataBTCMessage
from bxcommon.messages.btc.pong_btc_message import PongBTCMessage
from bxcommon.messages.btc.ver_ack_btc_message import VerAckBTCMessage
from bxcommon.messages.btc.version_btc_message import VersionBTCMessage
from bxcommon.messages.message import Message
from bxcommon.utils import logger
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.btc_message_parser import block_to_broadcastmsg, btc_tx_msg_to_tx_msg


# Connection from a bloXroute client to a BTC blockchain node
class BTCNodeConnection(GatewayConnection):
    ESTABLISHED = 0b1

    NONCE = random.randint(0, sys.maxint)  # Used to detect connections to self.

    def __init__(self, sock, address, node, from_me=False):
        super(BTCNodeConnection, self).__init__(sock, address, node)

        self.is_persistent = True
        magic_net = node.node_params['magic']
        self.magic = btc_magic_numbers[magic_net] if magic_net in btc_magic_numbers else int(magic_net)
        self.services = int(node.node_params['services'])
        self.version = node.node_params['version']
        self.protocol_version = int(node.node_params['protocol_version'])

        if 'nonce' not in node.node_params:
            node.node_params['nonce'] = random.randint(0, sys.maxint)

        self.nonce = node.node_params['nonce']

        # I must be the one that is establishing this connection.
        version_msg = VersionBTCMessage(self.magic, self.protocol_version, self.peer_ip, self.peer_port, self.my_ip,
                                        self.my_port, self.nonce, 0, self.version, self.services)
        self.enqueue_msg(version_msg)

        # Command to message handler for that function.
        self.message_handlers = {
            'ping': self.msg_ping,
            'pong': self.msg_pong,
            'version': self.msg_version,
            'block': self.msg_block,
            'tx': self.msg_tx,
            'getaddr': self.msg_getaddr,
            'inv': self.msg_inv,
        }

    def pop_next_message(self, payload_len, msg_type=Message, hdr_size=HDR_COMMON_OFF):
        return super(BTCNodeConnection, self).pop_next_message(payload_len, BTCMessage, BTC_HDR_COMMON_OFF)

    def process_message(self, msg_cls=Message, hello_msgs=['hello', 'ack']):
        return super(BTCNodeConnection, self).process_message(BTCMessage, ['version', 'verack'])

    ###
    # Handlers for each message type
    ###

    # Process ping message and send a pong.
    def msg_ping(self, msg):
        reply = PongBTCMessage(self.magic, msg.nonce())
        self.enqueue_msg(reply)

    # Ignore pong messages since we never send a ping.
    def msg_pong(self, msg):
        pass

    # Process incoming version message.
    # We are the node that initiated this connection, so we do not check for misbehavior.
    # Record that we received the version message, send a verack and synchronize chains if need be.
    def msg_version(self, msg):
        self.state |= ConnectionState.ESTABLISHED
        reply = VerAckBTCMessage(self.magic)
        self.enqueue_msg(reply)

        if self.state & ConnectionState.ESTABLISHED == ConnectionState.ESTABLISHED:
            for each_msg in self.node.node_msg_queue:
                self.enqueue_msg(each_msg)

            if self.node.node_msg_queue:
                self.node.node_msg_queue = deque()

            self.node.node_conn = self

    # Reply to GetAddr message with a blank Addr message to preserve privacy.
    def msg_getaddr(self, msg):
        reply = AddrBTCMessage(self.magic)
        self.enqueue_msg(reply)

    # Since I am only connected to this one node, we assume that everything in this inv
    # message is new and we want the data
    def msg_inv(self, msg):
        getdata = GetDataBTCMessage(magic=msg.magic(), inv_vects=[x for x in msg])
        self.enqueue_msg(getdata)

    # Handle a tx message by broadcasting this to the entire network.
    def msg_tx(self, msg):
        blx_txmsg = btc_tx_msg_to_tx_msg(msg)

        # All connections outside of this one is a bloXroute server
        logger.debug("Broadcasting the transaction to peers")
        self.node.broadcast(blx_txmsg, self)
        self.node.tx_service.hash_to_contents[msg.tx_hash()] = msg.tx()

    # Handle a block message.
    def msg_block(self, msg):
        blx_blockmsg = block_to_broadcastmsg(msg, self.node.tx_service)
        logger.debug("Compressed block with hash {0} to size {1} from size {2}"
                     .format(msg.block_hash(), len(blx_blockmsg.rawbytes()), len(msg.rawbytes())))
        self.node.block_recovery_service.cancel_recovery_for_block(msg.block_hash())
        self.node.broadcast(blx_blockmsg, self)
