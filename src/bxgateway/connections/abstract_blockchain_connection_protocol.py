import time
from abc import ABCMeta

from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import ObjectHash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import gateway_constants
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service


class AbstractBlockchainConnectionProtocol(object):
    __metaclass__ = ABCMeta

    def __init__(self, connection):
        self.connection = connection

    def msg_tx(self, msg):
        """
        Handle a TX message by broadcasting to the entire network
        """
        bx_tx_messages = self.connection.message_converter.tx_to_bx_txs(msg, self.connection.network_num)

        for (bx_tx_message, tx_hash, tx_bytes) in bx_tx_messages:
            if self.connection.node.get_tx_service().has_transaction_contents(tx_hash):
                tx_stats.add_tx_by_hash_event(tx_hash,
                                              TransactionStatEventType.TX_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                                              network_num=self.connection.network_num,
                                              peer=self.connection.peer_desc)
                gateway_transaction_stats_service.log_duplicate_transaction_from_blockchain()
                continue

            tx_stats.add_tx_by_hash_event(tx_hash, TransactionStatEventType.TX_RECEIVED_FROM_BLOCKCHAIN_NODE,
                                          network_num=self.connection.network_num,
                                          peer=self.connection.peer_desc)
            gateway_transaction_stats_service.log_transaction_from_blockchain(tx_hash)

            # All connections outside of this one is a bloXroute server
            broadcast_peers = self.connection.node.broadcast(bx_tx_message, self.connection)
            tx_stats.add_tx_by_hash_event(tx_hash, TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
                                          network_num=self.connection.network_num,
                                          peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE),
                                                    broadcast_peers))
            self.connection.node.get_tx_service().set_transaction_contents(tx_hash, tx_bytes)

    def msg_block(self, msg):
        """
        Handle a block message. Sends to node for encryption, then broadcasts.
        """
        block_hash = msg.block_hash()
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE,
                                                  network_num=self.connection.network_num,
                                                  blockchain_protocol=self.connection.node.opts.blockchain_protocol,
                                                  blockchain_network=self.connection.node.opts.blockchain_network)

        if block_hash in self.connection.node.blocks_seen.contents:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                                                      network_num=self.connection.network_num)
            logger.debug("Have seen block {0} before. Ignoring.".format(block_hash))
            return

        max_time_offset = self.connection.node.opts.blockchain_block_interval * self.connection.node.opts.blockchain_ignore_block_interval_count
        if time.time() - msg.timestamp() >= max_time_offset:
            logger.debug("Received block {} more than {} seconds after it was created ({}). Ignoring."
                         .format(block_hash, max_time_offset, msg.timestamp()))
            return

        self.connection.node.blocks_seen.add(block_hash)
        self.connection.node.block_processing_service.queue_block_for_processing(msg, self.connection)

    def msg_proxy_request(self, msg):
        """
        Handle a chainstate request message.
        """
        self.connection.node.send_msg_to_remote_node(msg)

    def msg_proxy_response(self, msg):
        """
        Handle a chainstate response message.
        """
        self.connection.node.send_msg_to_node(msg)

    def send_key(self, block_hash):
        """
        Sends out the decryption key for a block hash.
        """
        key = self.connection.node.in_progress_blocks.get_encryption_key(block_hash)
        key_message = KeyMessage(ObjectHash(block_hash), self.connection.network_num, key)
        conns = self.connection.node.broadcast(key_message, self.connection)
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.ENC_BLOCK_KEY_SENT_FROM_GATEWAY_TO_NETWORK,
                                                  network_num=self.connection.network_num,
                                                  peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), conns))
