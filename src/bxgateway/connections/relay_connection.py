import hashlib
import time

from bxcommon import constants
from bxcommon.constants import BTC_SHA_HASH_LEN
from bxcommon.messages.get_txs_message import GetTxsMessage
from bxcommon.messages.hello_message import HelloMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import BTCObjectHash
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages import btc_message_parser

sha256 = hashlib.sha256


class RelayConnection(GatewayConnection):
    def __init__(self, sock, address, node, from_me=False):
        super(RelayConnection, self).__init__(sock, address, node, from_me=from_me)

        self.is_server = True

        hello_msg = HelloMessage(self.node.idx)
        self.enqueue_msg(hello_msg)

        # Command to message handler for that function.
        self.message_handlers = {
            'hello': self.msg_hello,
            'ack': self.msg_ack,
            'broadcast': self.msg_broadcast,
            'tx': self.msg_tx,
            'txs': self.msg_txs
        }

    # Handle a broadcast message
    def msg_broadcast(self, msg):
        blx_block, block_hash, unknown_sids, unknown_hashes = btc_message_parser.broadcastmsg_to_block(
            msg,
            self.node.tx_service)
        if blx_block is not None:
            logger.debug("Decoded block successfully- sending block to node")
            self.node.send_bytes_to_node(blx_block)
        else:
            self.node.block_recovery_service.add_block_msg(msg, block_hash, unknown_sids, unknown_hashes)

            all_unknown_sids = []
            all_unknown_sids.extend(unknown_sids)

            # retrieving sids of txs with unknown contents
            for tx_hash in unknown_hashes:
                tx_sid = self.node.tx_service.get_txid(tx_hash)
                all_unknown_sids.append(tx_sid)

            logger.debug("Block recovery: Sending GetTxsMessage to relay with {0} unknown tx short ids."
                         .format(len(all_unknown_sids)))
            get_unknown_txs_msg = GetTxsMessage(short_ids=all_unknown_sids)
            self.enqueue_msg(get_unknown_txs_msg)
            logger.debug("Block Recovery: Requesting ....")

    # Receive a transaction from the bloXroute network.
    def msg_tx(self, msg):
        # Verify that the hash is a correct representation of the contents
        hash_val = BTCObjectHash(sha256(sha256(msg.tx_val()).digest()).digest(), length=BTC_SHA_HASH_LEN)

        if hash_val != msg.tx_hash():
            logger.error("Got ill formed tx message from the bloXroute network")
            return

        logger.debug("Adding hash value to tx service and forwarding it to node")
        self.node.tx_service.hash_to_contents[hash_val] = msg.tx_val()

        self.node.block_recovery_service.check_missing_tx_hash(hash_val)
        self._msg_broadcast_retry()

        if self.node.node_conn is not None:
            btc_tx_msg = btc_message_parser.tx_msg_to_btc_tx_msg(msg, self.node.node_conn.magic)
            self.node.send_bytes_to_node(btc_tx_msg.rawbytes())

    def msg_txs(self, msg):

        txs = msg.get_txs()

        logger.debug("Block recovery: Txs details message received from server. Contains {0} txs."
                     .format(len(txs)))

        for tx in txs:

            tx_sid, tx_hash, tx = tx

            self.node.block_recovery_service.check_missing_sid(tx_sid)

            if self.node.tx_service.get_txid(tx_hash) == constants.NULL_TX_SID:
                self.node.tx_service.assign_tx_to_sid(tx_hash, tx_sid, time.time())

            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)

            if tx_hash not in self.node.tx_service.hash_to_contents:
                self.node.tx_service.hash_to_contents[tx_hash] = tx

        self._msg_broadcast_retry()

    def _msg_broadcast_retry(self):
        if self.node.block_recovery_service.recovered_msgs:
            for msg in self.node.block_recovery_service.recovered_msgs:
                logger.info("Block recovery: Received all unknown txs for a block. Broadcasting block message.")
                self.msg_broadcast(msg)

            logger.debug("Block recovery: Broadcasted all of the messages ready for retry.")
            self.node.block_recovery_service.clean_up_recovered_messages()
