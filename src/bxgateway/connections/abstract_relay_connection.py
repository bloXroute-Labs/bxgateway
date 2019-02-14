import datetime

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.constants import BLOXROUTE_HELLO_MESSAGES, HDR_COMMON_OFF
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto, logger, convert
from bxcommon.utils.object_hash import ObjectHash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service


class AbstractRelayConnection(InternalNodeConnection):
    CONNECTION_TYPE = ConnectionType.RELAY

    def __init__(self, sock, address, node, from_me=False):
        super(AbstractRelayConnection, self).__init__(sock, address, node, from_me=from_me)

        hello_msg = HelloMessage(protocol_version=self.protocol_version, network_num=self.network_num,
                                 node_id=self.node.opts.node_id)
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
                                    BlockStatEventType.ENC_BLOCK_RECEIVED_BY_GATEWAY_FROM_NETWORK,
                                    network_num=self.network_num,
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
            block_stats.add_block_event(msg,
                                        BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS,
                                        network_num=self.network_num)
            self._handle_decrypted_block(block, encrypted_block_hash_hex=convert.bytes_to_hex(msg_hash.binary))
        else:
            logger.debug("Received encrypted block. Storing.")
            self.node.in_progress_blocks.add_ciphertext(msg_hash, cipherblob)
            block_received_message = BlockReceivedMessage(msg_hash)
            conns = self.node.broadcast(block_received_message, self, connection_type=ConnectionType.GATEWAY)
            block_stats.add_block_event_by_block_hash(msg_hash,
                                                      BlockStatEventType.ENC_BLOCK_SENT_BLOCK_RECEIPT,
                                                      network_num=self.network_num,
                                                      peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE),
                                                                conns))

    def msg_key(self, message):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        key = message.key()
        msg_hash = message.msg_hash()

        block_stats.add_block_event_by_block_hash(msg_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_RECEIVED_BY_GATEWAY_FROM_NETWORK,
                                                  network_num=self.network_num,
                                                  peer=self.peer_desc,
                                                  connection_type=self.CONNECTION_TYPE)

        if self.node.in_progress_blocks.has_ciphertext_for_hash(msg_hash):
            logger.debug("Cipher text found. Decrypting and sending to node.")
            block = self.node.in_progress_blocks.decrypt_and_get_payload(msg_hash, key)
            block_stats.add_block_event_by_block_hash(msg_hash,
                                                      BlockStatEventType.ENC_BLOCK_DECRYPTED_SUCCESS,
                                                      network_num=self.network_num)
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
        network_num = msg.network_num()
        tx_val = msg.tx_val()

        if tx_hash in tx_service.txhash_to_sids and not short_id:
            gateway_transaction_stats_service.log_duplicate_transaction_from_relay()
            tx_stats.add_tx_by_hash_event(tx_hash,
                                          TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER_IGNORE_SEEN,
                                          network_num=network_num, short_id=short_id, peer=self.peer_desc)
            logger.debug("Transaction has already been seen.")
            return

        tx_stats.add_tx_by_hash_event(tx_hash, TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER,
                                      network_num=network_num, short_id=short_id, peer=self.peer_desc,
                                      is_compact_transaction=(tx_val == TxMessage.EMPTY_TX_VAL))
        gateway_transaction_stats_service.log_transaction_from_relay(tx_hash,
                                                                     short_id is not None,
                                                                     tx_val == TxMessage.EMPTY_TX_VAL)
        if short_id:
            tx_stats.add_tx_by_hash_event(tx_hash, TransactionStatEventType.TX_SHORT_ID_STORED_BY_GATEWAY,
                                          network_num=network_num, short_id=short_id)
            tx_service.assign_short_id(tx_hash, short_id)
            self.node.block_recovery_service.check_missing_sid(short_id)
        else:
            tx_stats.add_tx_by_hash_event(tx_hash, TransactionStatEventType.TX_SHORT_ID_EMPTY_IN_MSG_FROM_RELAY,
                                          network_num=network_num, short_id=short_id, peer=self.peer_desc)
            logger.info("transaction had no short id: {}".format(msg))

        if tx_hash in tx_service.txhash_to_contents:
            logger.debug("Transaction has been seen, but short id newly assigned.")
            if tx_val != TxMessage.EMPTY_TX_VAL:
                tx_stats.add_tx_by_hash_event(tx_hash,
                                              TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER_IGNORE_SEEN,
                                              network_num=network_num, short_id=short_id, peer=self.peer_desc)
                gateway_transaction_stats_service.log_redundant_transaction_content()
            return

        if tx_val != TxMessage.EMPTY_TX_VAL:
            logger.debug("Adding hash value to tx service and forwarding it to node")
            tx_service.txhash_to_contents[tx_hash] = msg.tx_val()
            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)
            self._msg_broadcast_retry()

            if self.node.node_conn is not None:
                btc_tx_msg = self.message_converter.bx_tx_to_tx(msg)
                self.node.send_msg_to_node(btc_tx_msg)

            tx_stats.add_tx_by_hash_event(tx_hash, TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_BLOCKCHAIN_NODE,
                                          network_num=network_num, short_id=short_id)

    def msg_txs(self, msg):
        transactions = msg.get_txs()
        tx_service = self.node.get_tx_service()

        tx_stats.add_txs_by_short_ids_event(map(lambda x: x[0], transactions),
                                            TransactionStatEventType.TX_UNKNOWN_SHORT_IDS_REPLY_RECEIVED_BY_GATEWAY_FROM_RELAY,
                                            network_num=self.node.network_num,
                                            peer=self.peer_desc,
                                            found_tx_hashes=map(lambda x: convert.bytes_to_hex(x[1].binary),
                                                                transactions))

        for transaction in transactions:
            short_id, tx_hash, transaction_contents = transaction

            self.node.block_recovery_service.check_missing_sid(short_id)

            if short_id not in tx_service.sid_to_txhash:
                tx_service.assign_short_id(tx_hash, short_id)

            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)

            if tx_hash not in tx_service.txhash_to_contents:
                tx_service.txhash_to_contents[tx_hash] = transaction_contents

        self._msg_broadcast_retry()

    def _msg_broadcast_retry(self):
        if self.node.block_recovery_service.recovered_blocks:
            for msg in self.node.block_recovery_service.recovered_blocks:
                self._handle_decrypted_block(msg, recovered=True)

            self.node.block_recovery_service.clean_up_recovered_blocks()

    def _handle_decrypted_block(self, bx_block, encrypted_block_hash_hex=None, recovered=False):
        decompress_start = datetime.datetime.utcnow()
        # TODO: determine if a real block or test block. Discard if test block.
        block_message, block_hash, unknown_sids, unknown_hashes = \
            self.message_converter.bx_block_to_block(bx_block, self.node.get_tx_service())
        decompress_end = datetime.datetime.utcnow()

        if recovered:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECOVERY_COMPLETED,
                                                      network_num=self.network_num)

        if encrypted_block_hash_hex is None:
            encrypted_block_hash_hex = "Unknown"

        if block_hash in self.node.blocks_seen.contents:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_IGNORE_SEEN,
                                                      start_date_time=decompress_start,
                                                      end_date_time=decompress_end,
                                                      network_num=self.network_num,
                                                      encrypted_block_hash=encrypted_block_hash_hex)
            return

        if block_message is not None:
            block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_DECOMPRESSED_SUCCESS,
                                                      start_date_time=decompress_start,
                                                      end_date_time=decompress_end,
                                                      network_num=self.network_num,
                                                      encrypted_block_hash=encrypted_block_hash_hex)
            if recovered or block_hash in self.node.block_queuing_service:
                self.node.block_queuing_service.update_recovered_block(block_hash, block_message)
            else:
                self.node.block_queuing_service.push(block_hash, block_message)

            self.node.block_recovery_service.cancel_recovery_for_block(block_hash)
            self.node.blocks_seen.add(block_hash)
        else:
            if block_hash in self.node.block_queuing_service and not recovered:
                logger.debug("Handling already queued block again. Ignoring.")
                return

            self.node.block_recovery_service.add_block(bx_block, block_hash, unknown_sids, unknown_hashes)
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_DECOMPRESSED_WITH_UNKNOWN_TXS,
                                                      start_date_time=decompress_start,
                                                      end_date_time=decompress_end,
                                                      network_num=self.network_num,
                                                      encrypted_block_hash=encrypted_block_hash_hex,
                                                      unknown_sids_count=len(unknown_sids),
                                                      unknown_hashes_count=len(unknown_hashes))
            gettxs_message = self._create_unknown_txs_message(unknown_sids, unknown_hashes)
            self.enqueue_msg(gettxs_message)
            tx_stats.add_txs_by_short_ids_event(unknown_sids,
                                                TransactionStatEventType.TX_UNKNOWN_SHORT_IDS_REQUESTED_BY_GATEWAY_FROM_RELAY,
                                                network_num=self.node.network_num,
                                                peer=self.peer_desc,
                                                block_hash=convert.bytes_to_hex(block_hash.binary))

            if recovered:
                # for now, just allow retry.
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_RECOVERY_REPEATED,
                                                          network_num=self.network_num,
                                                          request_hash=convert.bytes_to_hex(
                                                              crypto.double_sha256(gettxs_message.rawbytes())
                                                          ))
            else:
                self.node.block_queuing_service.push(block_hash, waiting_for_recovery=True)
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_RECOVERY_STARTED,
                                                          network_num=self.network_num,
                                                          request_hash=convert.bytes_to_hex(
                                                              crypto.double_sha256(gettxs_message.rawbytes())))

    def _create_unknown_txs_message(self, unknown_sids, unknown_hashes):
        all_unknown_sids = []
        all_unknown_sids.extend(unknown_sids)

        tx_service = self.node.get_tx_service()

        # retrieving sids of txs with unknown contents
        for tx_hash in unknown_hashes:
            tx_sid = tx_service.get_short_id(tx_hash)
            all_unknown_sids.append(tx_sid)

        return GetTxsMessage(short_ids=all_unknown_sids)
