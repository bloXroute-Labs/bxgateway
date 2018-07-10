import hashlib

from bxcommon.constants import BTC_SHA_HASH_LEN, BTC_HDR_COMMON_OFF
from bxcommon.messages.btc.btc_message import BTCMessage
from bxcommon.messages.hello_message import HelloMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import BTCObjectHash
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.btc_message_parser import broadcastmsg_to_block

sha256 = hashlib.sha256


class RelayConnection(GatewayConnection):
    def __init__(self, sock, address, node, from_me=False, setup=False):
        super(RelayConnection, self).__init__(sock, address, node, setup=setup)

        self.is_server = True
        self.is_persistent = True

        hello_msg = HelloMessage(self.node.idx)
        self.enqueue_msg(hello_msg)

        # Command to message handler for that function.
        self.message_handlers = {
            'hello': self.msg_hello,
            'ack': self.msg_ack,
            'broadcast': self.msg_broadcast,
            'txassign': self.msg_txassign,
            'tx': self.msg_tx
        }

    ###
    # Handlers for each message type
    ###

    # Handle a broadcast message
    def msg_broadcast(self, msg):
        blx_block = broadcastmsg_to_block(msg, self.node.tx_manager)
        if blx_block is not None:
            logger.debug("Decoded block successfully- sending block to node")
            self.node.send_bytes_to_node(blx_block)
        else:
            logger.debug("Failed to decode blx block. Dropping")

    # Receive a transaction from the bloXroute network.
    def msg_tx(self, msg):
        hash_val = BTCObjectHash(sha256(sha256(msg.blob()).digest()).digest(), length=BTC_SHA_HASH_LEN)

        if hash_val != msg.msg_hash():
            logger.error("Got ill formed tx message from the bloXroute network")
            return

        logger.debug("Adding hash value to tx manager and forwarding it to node")
        self.node.tx_manager.hash_to_contents[hash_val] = msg.blob()
        # XXX: making a copy here- should make this more efficient
        # XXX: maybe by sending the full tx message in a block...
        # XXX: this should eventually be moved into the parser.
        buf = bytearray(BTC_HDR_COMMON_OFF) + msg.blob()
        if self.node.node_conn is not None:
            txmsg = BTCMessage(self.node.node_conn.magic, 'tx', len(msg.blob()), buf)
            self.node.send_bytes_to_node(txmsg.rawbytes())
