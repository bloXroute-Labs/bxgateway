import datetime
import time

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.constants import BLOXROUTE_HELLO_MESSAGES, HDR_COMMON_OFF, NULL_TX_SID
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.utils import crypto, logger, convert
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats


class AbstractRelayConnection(InternalNodeConnection):
    CONNECTION_TYPE = ConnectionType.RELAY

    def __init__(self, sock, address, node, from_me=False):
        super(AbstractRelayConnection, self).__init__(sock, address, node, from_me=from_me)

        hello_msg = HelloMessage(protocol_version=self.protocol_version, idx=self.node.idx,
                                 network_num=self.network_num)
        self.enqueue_msg(hello_msg)

        self.hello_messages = BLOXROUTE_HELLO_MESSAGES
        self.header_size = HDR_COMMON_OFF
        self.message_handlers = {
            BloxrouteMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.ACK: self.msg_ack,
            BloxrouteMessageType.BROADCAST: self.msg_broadcast,
            BloxrouteMessageType.KEY: self.msg_key,
            BloxrouteMessageType.TRANSACTION: self.msg_tx,
            BloxrouteMessageType.TRANSACTIONS: self.msg_txs,
        }

        self.message_converter = None

    def msg_broadcast(self, msg):
        """
        Handle broadcast message receive from bloXroute.
        This is typically an encrypted block.
        """

        block_stats.add_block_event(msg,
                                    BlockStatEventType.ENC_BLOCK_RECEIVED_BY_GATEWAY_FROM_PEER,
                                    peer=self.peer_desc,
                                    connection_type=self.CONNECTION_TYPE)

        msg_hash = msg.msg_hash()

        cipherblob = msg.blob()
        if msg_hash != ObjectHash(crypto.double_sha256(cipherblob)):
            logger.warn("Received a message with inconsistent hashes. Dropping.")
            return
        if self.node.in_progress_blocks.has_encryption_key_for_hash(msg_hash):
            logger.debug("Already had key for received block. Sending block to node.")
            block = self.node.in_progress_blocks.decrypt_ciphertext(msg_hash, cipherblob)
            block_stats.add_block_event(msg, BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS)
            self._handle_decrypted_block(block, encrypted_block_hash_hex=convert.bytes_to_hex(msg_hash.binary))
        else:
            logger.debug("Received encrypted block. Storing.")
            self.node.in_progress_blocks.add_ciphertext(msg_hash, cipherblob)
            block_received_message = BlockReceivedMessage(msg_hash)
            self.node.broadcast(block_received_message, self, connection_type=ConnectionType.GATEWAY)

    def msg_key(self, message):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        key = message.key()
        msg_hash = message.msg_hash()

        block_stats.add_block_event_by_block_hash(msg_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_RECEIVED_BY_GATEWAY_FROM_PEER,
                                                  peer=self.peer_desc,
                                                  connection_type=self.CONNECTION_TYPE)

        if self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash):
            logger.debug("Cipher text found. Decrypting and sending to node.")
            block = self.node.in_progress_blocks.decrypt_and_get_payload(msg_hash, key)
            block_stats.add_block_event_by_block_hash(msg_hash, BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS)
            self._handle_decrypted_block(block, encrypted_block_hash_hex=convert.bytes_to_hex(msg_hash.binary))
        else:
            logger.debug("No cipher text found on key message. Storing.")
            self.node.in_progress_blocks.add_key(msg_hash, key)

    def msg_tx(self, msg):
        """
        Handle transactions receive from bloXroute network.
        """
        tx_service = self.node.get_tx_service()

        short_id = msg.short_id()
        tx_hash = msg.tx_hash()

        if tx_hash in tx_service.txhash_to_sid:
            logger.debug("Transaction has assigned short id!")
            return

        if short_id:
            tx_service.assign_tx_to_sid(tx_hash, short_id, time.time())

        if tx_hash in tx_service.hash_to_contents:
            logger.debug("Transaction has been seen, but short id newly assigned.")
            return

        logger.debug("Adding hash value to tx service and forwarding it to node")
        tx_service.hash_to_contents[tx_hash] = msg.tx_val()

        self.node.block_recovery_service.check_missing_tx_hash(tx_hash)
        self._msg_broadcast_retry()

        if self.node.node_conn is not None:
            btc_tx_msg = self.message_converter.bx_tx_to_tx(msg)
            self.node.send_msg_to_node(btc_tx_msg)

    def msg_txs(self, msg):

        txs = msg.get_txs()

        logger.debug("Block recovery: Txs details message received from server. Contains {0} txs."
                     .format(len(txs)))

        tx_service = self.node.get_tx_service()

        for tx in txs:

            tx_sid, tx_hash, tx = tx

            self.node.block_recovery_service.check_missing_sid(tx_sid)

            if tx_service.get_txid(tx_hash) == NULL_TX_SID:
                tx_service.assign_tx_to_sid(tx_hash, tx_sid, time.time())

            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)

            if tx_hash not in tx_service.hash_to_contents:
                tx_service.hash_to_contents[tx_hash] = tx

        self._msg_broadcast_retry()

    def _msg_broadcast_retry(self):
        if self.node.block_recovery_service.recovered_blocks:
            for msg in self.node.block_recovery_service.recovered_blocks:
                logger.info("Block recovery: Received all unknown txs for a block. Broadcasting block message.")
                self._handle_decrypted_block(msg, recovered=True)

            logger.debug("Block recovery: Broadcasted all of the messages ready for retry.")
            self.node.block_recovery_service.clean_up_recovered_blocks()

    def _handle_decrypted_block(self, blx_block, encrypted_block_hash_hex=None, recovered=False):
        decompress_start = datetime.datetime.utcnow()
        # TODO: determine if a real block or test block. Discard if test block.
        btc_block, block_hash, unknown_sids, unknown_hashes = \
            self.message_converter.bx_block_to_block(blx_block, self.node.get_tx_service())
        decompress_end = datetime.datetime.utcnow()

        if recovered:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_RECOVERY_COMPLETED)

        if encrypted_block_hash_hex is None:
            encrypted_block_hash_hex = "Unknown"

        if block_hash in self.node.blocks_seen.contents:
            logger.warn("Already saw block {0}. Dropping!".format(hash))
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_IGNORE_SEEN,
                                                      start_date_time=decompress_start,
                                                      end_date_time=decompress_end,
                                                      encrypted_block_hash=encrypted_block_hash_hex)
            return

        if btc_block is not None:
            logger.debug("Decoded block successfully- sending block to node")
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_SUCCESS,
                                                      start_date_time=decompress_start,
                                                      end_date_time=decompress_end,
                                                      encrypted_block_hash=encrypted_block_hash_hex)
            self.node.send_msg_to_node(btc_block)
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE)
            self.node.blocks_seen.add(block_hash)
        else:
            self.node.block_recovery_service.add_block(blx_block, block_hash, unknown_sids, unknown_hashes)
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_DECOMPRESSED_WITH_UNKNOWN_TXS,
                                                      start_date_time=decompress_start,
                                                      end_date_time=decompress_end,
                                                      encrypted_block_hash=encrypted_block_hash_hex,
                                                      unknown_sids_count=len(unknown_sids),
                                                      unknown_hashes_count=len(unknown_hashes))
            self.enqueue_msg(self._create_unknown_txs_message(unknown_sids, unknown_hashes))
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_RECOVERY_STARTED)
            logger.debug("Block Recovery: Requesting ....")

    def _create_unknown_txs_message(self, unknown_sids, unknown_hashes):
        all_unknown_sids = []
        all_unknown_sids.extend(unknown_sids)

        tx_service = self.node.get_tx_service()

        # retrieving sids of txs with unknown contents
        for tx_hash in unknown_hashes:
            tx_sid = tx_service.get_txid(tx_hash)
            all_unknown_sids.append(tx_sid)

        logger.debug("Block recovery: Sending GetTxsMessage to relay with {0} unknown tx short ids."
                     .format(len(all_unknown_sids)))
        return GetTxsMessage(short_ids=all_unknown_sids)
