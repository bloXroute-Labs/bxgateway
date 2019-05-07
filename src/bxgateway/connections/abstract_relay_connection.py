from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.constants import BLOXROUTE_HELLO_MESSAGES, HDR_COMMON_OFF
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import logger, convert
from bxcommon.utils.buffers.output_buffer import OutputBuffer
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service


class AbstractRelayConnection(InternalNodeConnection):
    CONNECTION_TYPE = ConnectionType.RELAY

    def __init__(self, sock, address, node, from_me=False):
        super(AbstractRelayConnection, self).__init__(sock, address, node, from_me=from_me)

        # TODO: Temporarily disable buffering for gateway to relay connection.
        #       Need to enable buffering only for transactions and disable for blocks
        self.outputbuf = OutputBuffer()

        hello_msg = HelloMessage(protocol_version=self.protocol_version, network_num=self.network_num,
                                 node_id=self.node.opts.node_id)
        self.enqueue_msg(hello_msg)

        self.hello_messages = BLOXROUTE_HELLO_MESSAGES
        self.header_size = HDR_COMMON_OFF
        self.message_handlers = {
            BloxrouteMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.PING: self.msg_ping,
            BloxrouteMessageType.PONG: self.msg_pong,
            BloxrouteMessageType.ACK: self.msg_ack,
            BloxrouteMessageType.BROADCAST: self.msg_broadcast,
            BloxrouteMessageType.KEY: self.msg_key,
            BloxrouteMessageType.TRANSACTION: self.msg_tx,
            BloxrouteMessageType.TRANSACTIONS: self.msg_txs,
            BloxrouteMessageType.BLOCK_HOLDING: self.msg_block_holding
        }

        self.message_converter = None

    def msg_broadcast(self, msg):
        """
        Handle broadcast message receive from bloXroute.
        This is typically an encrypted block.
        """
        self.node.block_processing_service.process_block_broadcast(msg, self)

    def msg_key(self, msg):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        self.node.block_processing_service.process_block_key(msg, self)

    def msg_tx(self, msg):
        """
        Handle transactions receive from bloXroute network.
        """
        tx_service = self.node.get_tx_service()

        short_id = msg.short_id()
        tx_hash = msg.tx_hash()
        network_num = msg.network_num()
        tx_val = msg.tx_val()

        if tx_service.has_transaction_short_id(tx_hash) and not short_id:
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

        if tx_service.has_transaction_contents(tx_hash):
            logger.debug("Transaction has been seen, but short id newly assigned.")
            if tx_val != TxMessage.EMPTY_TX_VAL:
                tx_stats.add_tx_by_hash_event(tx_hash,
                                              TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER_IGNORE_SEEN,
                                              network_num=network_num, short_id=short_id, peer=self.peer_desc)
                gateway_transaction_stats_service.log_redundant_transaction_content()
            return

        if tx_val != TxMessage.EMPTY_TX_VAL:
            logger.debug("Adding hash value to tx service and forwarding it to node")
            tx_service.set_transaction_contents(tx_hash, msg.tx_val())
            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)
            self.node.block_processing_service.retry_broadcast_recovered_blocks(self)

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
            tx_hash, transaction_contents, short_id = transaction

            self.node.block_recovery_service.check_missing_sid(short_id)

            if not tx_service.has_short_id(short_id):
                tx_service.assign_short_id(tx_hash, short_id)

            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)

            if not tx_service.has_transaction_contents(tx_hash):
                tx_service.set_transaction_contents(tx_hash, transaction_contents)

        self.node.block_processing_service.retry_broadcast_recovered_blocks(self)

    def msg_block_holding(self, msg):
        """
        Block holding request message handler. Places block on hold and broadcasts message to relay and gateway peers.
        :param msg: Message of type BlockHoldingMessage
        """
        block_hash = msg.block_hash()
        self.node.block_processing_service.place_hold(block_hash, self)
