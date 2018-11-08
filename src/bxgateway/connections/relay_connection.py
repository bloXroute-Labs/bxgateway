import time

from bxcommon.constants import BTC_SHA_HASH_LEN, BLOXROUTE_HELLO_MESSAGES, HDR_COMMON_OFF, NULL_TX_SID
from bxcommon.messages.bloxroute.bloxroute_message_factory import bloxroute_message_factory
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.utils import logger, crypto
from bxcommon.utils.object_hash import BTCObjectHash, ObjectHash
from bxgateway.connections.gateway_connection import GatewayConnection
from bxgateway.messages import btc_message_parser


class RelayConnection(GatewayConnection):
    def __init__(self, sock, address, node, from_me=False):
        super(RelayConnection, self).__init__(sock, address, node, from_me=from_me)

        self.is_server = True

        hello_msg = HelloMessage(self.node.idx)
        self.enqueue_msg(hello_msg)

        self.hello_messages = BLOXROUTE_HELLO_MESSAGES
        self.header_size = HDR_COMMON_OFF
        self.message_factory = bloxroute_message_factory
        self.message_handlers = {
            BloxrouteMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.ACK: self.msg_ack,
            BloxrouteMessageType.BROADCAST: self.msg_broadcast,
            BloxrouteMessageType.KEY: self.msg_key,
            BloxrouteMessageType.TRANSACTION: self.msg_tx,
            BloxrouteMessageType.TRANSACTIONS: self.msg_txs,
        }

    def msg_broadcast(self, msg):
        """
        Handle broadcast message receive from bloXroute.
        This is typically an encrypted block.
        """
        msg_hash = msg.msg_hash()
        cipherblob = msg.blob()
        if msg_hash != ObjectHash(crypto.double_sha256(cipherblob)):
            logger.warn("Received a message with inconsistent hashes. Dropping.")
            return
        if self.node.in_progress_blocks.has_encryption_key_for_hash(msg_hash):
            logger.debug("Already had key for received block. Sending block to node.")
            block = self.node.in_progress_blocks.decrypt_ciphertext(msg_hash, cipherblob)
            self._handle_block(block)
        else:
            logger.debug("Received encrypted block. Storing.")
            self.node.in_progress_blocks.add_ciphertext(msg_hash, cipherblob)

    def msg_key(self, message):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        key = message.key()
        msg_hash = message.msg_hash()
        if self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash):
            logger.debug("Cipher text found. Decrypting and sending to node.")
            block = self.node.in_progress_blocks.decrypt_and_get_payload(msg_hash, key)
            self._handle_block(block)
        else:
            logger.debug("No cipher text found on key message. Storing.")
            self.node.in_progress_blocks.add_key(msg_hash, key)

    def msg_tx(self, msg):
        """
        Handle transactions receive from bloXroute network.
        """
        hash_val = BTCObjectHash(crypto.bitcoin_hash(msg.tx_val()), length=BTC_SHA_HASH_LEN)

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

            if self.node.tx_service.get_txid(tx_hash) == NULL_TX_SID:
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

    def _handle_block(self, message):
        # TODO: determine if a real block or test block. Discard if test block.
        btc_block, block_hash, unknown_sids, unknown_hashes = \
            btc_message_parser.bloxroute_block_to_btc_block(message, self.node.tx_service)
        if btc_block is not None:
            logger.debug("Decoded block successfully- sending block to node")
            self.node.send_bytes_to_node(btc_block.rawbytes())
        else:
            self.node.block_recovery_service.add_block_msg(message, block_hash, unknown_sids, unknown_hashes)
            self.enqueue_msg(self._create_unknown_txs_message(unknown_sids, unknown_hashes))
            logger.debug("Block Recovery: Requesting ....")

    def _create_unknown_txs_message(self, unknown_sids, unknown_hashes):
        all_unknown_sids = []
        all_unknown_sids.extend(unknown_sids)

        # retrieving sids of txs with unknown contents
        for tx_hash in unknown_hashes:
            tx_sid = self.node.tx_service.get_txid(tx_hash)
            all_unknown_sids.append(tx_sid)

        logger.debug("Block recovery: Sending GetTxsMessage to relay with {0} unknown tx short ids."
                     .format(len(all_unknown_sids)))
        return GetTxsMessage(short_ids=all_unknown_sids)
