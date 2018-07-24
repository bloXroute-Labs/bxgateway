import hashlib
import time

from bxcommon.constants import BTC_SHA_HASH_LEN
from bxcommon.messages.get_txs_details_message import GetTxsDetailsMessage
from bxcommon.messages.hello_message import HelloMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import BTCObjectHash
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages.btc_message_parser import broadcastmsg_to_block, tx_msg_to_btc_tx_msg

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
            'tx': self.msg_tx,
            'txs': self.msg_txs_details
        }

    # Handle a broadcast message
    def msg_broadcast(self, msg):
        blx_block, block_hash, unknown_sids, unknown_hashes = broadcastmsg_to_block(msg, self.node.tx_service)
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

            get_unknown_txs_msg = GetTxsDetailsMessage(short_ids=all_unknown_sids)
            self.enqueue_msg(get_unknown_txs_msg)
            logger.debug("Block Recovery: Requesting ....")

    # Receive a transaction from the bloXroute network.
    def msg_tx(self, msg):
        hash_val = BTCObjectHash(sha256(sha256(msg.blob()).digest()).digest(), length=BTC_SHA_HASH_LEN)

        if hash_val != msg.msg_hash():
            logger.error("Got ill formed tx message from the bloXroute network")
            return

        logger.debug("Adding hash value to tx manager and forwarding it to node")
        self.node.tx_service.hash_to_contents[hash_val] = msg.blob()

        self.node.block_recovery_service.check_missing_tx_hash(hash_val)
        self._msg_broadcast_retry()

        if self.node.node_conn is not None:
            btc_tx_msg = tx_msg_to_btc_tx_msg(msg, self.node.node_conn.magic)
            self.node.send_bytes_to_node(btc_tx_msg.rawbytes())

    def msg_txassign(self, msg):
        tx_hash = super(RelayConnection, self).msg_txassign(msg)

        if tx_hash is not None:
            self.node.block_recovery_service.check_missing_sid(msg.short_id())
            self._msg_broadcast_retry()

        return tx_hash

    def msg_txs_details(self, msg):

        txs_info = msg.get_txs_details()

        logger.debug("Block recovery: Txs details message received from server. Contains {0} txs."
                     .format(len(txs_info)))

        for tx_info in txs_info:

            tx_sid = tx_info[0]
            tx_hash = tx_info[1]
            tx = tx_info[2]

            self.node.block_recovery_service.check_missing_sid(tx_sid)

            if self.node.tx_service.get_txid(tx_hash) == -1:
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
