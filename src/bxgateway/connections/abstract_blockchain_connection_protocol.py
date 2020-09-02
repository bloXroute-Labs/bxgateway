import time
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Union

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.models.tx_validation_status import TxValidationStatus
from bxcommon.utils import performance_utils
from bxcommon.utils.blockchain_utils.eth import eth_common_utils, transaction_validation_utils
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxutils import logging, log_messages
from bxutils.logging.log_record_type import LogRecordType

logger = logging.get_logger(__name__)
msg_handling_logger = logging.get_logger(LogRecordType.MessageHandlingTroubleshooting, __name__)


class AbstractBlockchainConnectionProtocol:
    __metaclass__ = ABCMeta
    connection: AbstractGatewayBlockchainConnection

    def __init__(
        self,
        connection: AbstractGatewayBlockchainConnection,
        block_cleanup_poll_interval_s: int = gateway_constants.BLOCK_CLEANUP_NODE_BLOCK_LIST_POLL_INTERVAL_S,
    ):
        self.block_cleanup_poll_interval_s = block_cleanup_poll_interval_s
        self.connection = connection

    def msg_tx(self, msg):
        """
        Handle a TX message by broadcasting to the entire network
        """
        start_time = time.time()
        txn_count = 0
        broadcast_txs_count = 0

        tx_service = self.connection.node.get_tx_service()
        process_tx_msg_result = tx_service.process_transactions_message_from_node(
            msg,
            self.connection.node.get_network_min_transaction_fee(),
            self.connection.node.opts.transaction_validation
        )

        if not self.connection.node.opts.has_fully_updated_tx_service:
            logger.debug(
                "Gateway received {} transaction messages while syncing, skipping..",
                len(process_tx_msg_result)
            )
            return

        broadcast_start_time = time.time()

        for tx_result in process_tx_msg_result:
            txn_count += 1
            if TxValidationStatus.INVALID_FORMAT in tx_result.tx_validation_status:
                gateway_transaction_stats_service.log_tx_validation_failed_structure()
                logger.warning(
                    log_messages.NODE_RECEIVED_TX_WITH_INVALID_FORMAT,
                    self.connection.node.NODE_TYPE,
                    tx_result.transaction_hash,
                    stats_format.connection(self.connection)
                )

                tx_stats.add_tx_by_hash_event(
                    tx_result.transaction_hash,
                    TransactionStatEventType.TX_VALIDATION_FAILED_STRUCTURE,
                    self.connection.network_num,
                    peer=stats_format.connection(self.connection)
                )
                continue

            if TxValidationStatus.INVALID_SIGNATURE in tx_result.tx_validation_status:
                gateway_transaction_stats_service.log_tx_validation_failed_signature()
                logger.warning(
                    log_messages.NODE_RECEIVED_TX_WITH_INVALID_SIG,
                    self.connection.node.NODE_TYPE,
                    tx_result.transaction_hash,
                    stats_format.connection(self.connection)
                )

                tx_stats.add_tx_by_hash_event(
                    tx_result.transaction_hash,
                    TransactionStatEventType.TX_VALIDATION_FAILED_SIGNATURE,
                    self.connection.network_num,
                    peer=stats_format.connection(self.connection)
                )
                continue

            if TxValidationStatus.LOW_FEE in tx_result.tx_validation_status:
                gateway_transaction_stats_service.log_tx_validation_failed_gas_price()
                # log low fee transaction here for bdn_performance
                gateway_bdn_performance_stats_service.log_tx_from_blockchain_node(True)

                logger.trace(
                    "transaction {} has gas price lower then the setting {}",
                    tx_result.transaction_hash,
                    self.connection.node.get_network_min_transaction_fee()
                )

                tx_stats.add_tx_by_hash_event(
                    tx_result.transaction_hash,
                    TransactionStatEventType.TX_VALIDATION_FAILED_GAS_PRICE,
                    self.connection.network_num,
                    peer=stats_format.connection(self.connection)
                )
                continue

            if tx_result.seen:
                tx_stats.add_tx_by_hash_event(
                    tx_result.transaction_hash,
                    TransactionStatEventType.TX_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                    self.connection.network_num,
                    peer=stats_format.connection(self.connection)
                )
                gateway_transaction_stats_service.log_duplicate_transaction_from_blockchain()
                continue

            broadcast_txs_count += 1

            tx_stats.add_tx_by_hash_event(
                tx_result.transaction_hash,
                TransactionStatEventType.TX_RECEIVED_FROM_BLOCKCHAIN_NODE,
                self.connection.network_num,
                peer=stats_format.connection(self.connection)
            )
            gateway_transaction_stats_service.log_transaction_from_blockchain(tx_result.transaction_hash)

            # log transactions that passed validation, according to fee
            gateway_bdn_performance_stats_service.log_tx_from_blockchain_node(
                not self.node.is_gas_price_above_min_network_fee(tx_result.transaction_contents)
            )

            # All connections outside of this one is a bloXroute server
            broadcast_peers = self.connection.node.broadcast(
                tx_result.bdn_transaction_message,
                self.connection,
                connection_types=[ConnectionType.RELAY_TRANSACTION]
            )
            if self.connection.node.opts.ws:
                self.publish_transaction(
                    tx_result.transaction_hash, memoryview(tx_result.transaction_contents)
                )

            if broadcast_peers:
                tx_stats.add_tx_by_hash_event(
                    tx_result.transaction_hash,
                    TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
                    self.connection.network_num,
                    peers=map(lambda conn: (stats_format.connection(conn)), broadcast_peers)
                )
            else:
                logger.trace(
                    "Tx Message: {} from BlockchainNode was dropped, no upstream relay connection available",
                    tx_result.transaction_hash
                )

        set_content_start_time = time.time()
        end_time = time.time()

        total_duration_ms = (end_time - start_time) * 1000
        duration_before_broadcast_ms = (broadcast_start_time - start_time) * 1000
        duration_broadcast_ms = (set_content_start_time - broadcast_start_time) * 1000
        duration_set_content_ms = (end_time - set_content_start_time) * 1000

        gateway_transaction_stats_service.log_processed_node_transaction(
            total_duration_ms,
            duration_before_broadcast_ms,
            duration_broadcast_ms,
            duration_set_content_ms,
            broadcast_txs_count
        )

        performance_utils.log_operation_duration(
            msg_handling_logger,
            "Process single transaction from Blockchain",
            start_time,
            gateway_constants.BLOCKCHAIN_TX_PROCESSING_TIME_WARNING_THRESHOLD_S,
            connection=self,
            message=msg,
            total_duration_ms=total_duration_ms,
            duration_before_broadcast_ms=duration_before_broadcast_ms,
            duration_broadcast_ms=duration_broadcast_ms,
            duration_set_content_ms=duration_set_content_ms
        )

    @abstractmethod
    def msg_block(self, msg: AbstractBlockMessage) -> None:
        """
        Handle a block message. Sends to node for encryption, then broadcasts.
        """
        pass

    def process_msg_block(self, msg: AbstractBlockMessage, block_number: Optional[int] = None) -> None:
        block_hash = msg.block_hash()
        node = self.connection.node
        # if gateway is still syncing, skip this process
        if not node.should_process_block_hash(block_hash):
            return

        node.block_cleanup_service.on_new_block_received(block_hash, msg.prev_block_hash())
        block_stats.add_block_event_by_block_hash(
            block_hash, 
            BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE,
            network_num=self.connection.network_num,
            more_info="Protocol: {}, Network: {}".format(
                node.opts.blockchain_protocol,
                node.opts.blockchain_network,
                msg.extra_stats_data()
            )
        )
        if block_hash in self.connection.node.blocks_seen.contents:
            node.on_block_seen_by_blockchain_node(block_hash, block_number=block_number)
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                network_num=self.connection.network_num
            )
            self.connection.log_info(
                "Discarding duplicate block {} from local blockchain node.",
                block_hash
            )
            return

        if not self.is_valid_block_timestamp(msg):
            return

        canceled_recovery = node.on_block_seen_by_blockchain_node(block_hash, msg)
        if canceled_recovery:
            return

        node.track_block_from_node_handling_started(block_hash)
        node.on_block_seen_by_blockchain_node(block_hash, msg, block_number=block_number)
        node.block_processing_service.queue_block_for_processing(msg, self.connection)
        gateway_bdn_performance_stats_service.log_block_from_blockchain_node()
        node.block_queuing_service.store_block_data(block_hash, msg)
        return

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

    def is_valid_block_timestamp(self, msg: AbstractBlockMessage) -> bool:
        max_time_offset = (
            self.connection.node.opts.blockchain_block_interval *
            self.connection.node.opts.blockchain_ignore_block_interval_count
        )
        if time.time() - msg.timestamp() >= max_time_offset:
            self.connection.log_trace("Received block {} more than {} seconds after it was created ({}). Ignoring.",
                                      msg.block_hash(), max_time_offset, msg.timestamp())
            return False

        return True

    def _request_blocks_confirmation(self):
        if not self.connection.is_alive():
            return None
        node = self.connection.node
        last_confirmed_block = node.block_cleanup_service.last_confirmed_block
        tracked_blocks = node.get_tx_service().get_oldest_tracked_block(node.network.block_confirmations_count)

        if last_confirmed_block is not None and len(tracked_blocks) <= node.network.block_confirmations_count + \
            gateway_constants.BLOCK_CLEANUP_REQUEST_EXPECTED_ADDITIONAL_TRACKED_BLOCKS:
            hashes = [last_confirmed_block]
            hashes.extend(tracked_blocks)
        else:
            hashes = tracked_blocks
        if hashes:
            msg = self._build_get_blocks_message_for_block_confirmation(hashes)
            self.connection.enqueue_msg(msg)
            self.connection.log_debug("Sending block confirmation request. Last confirmed block: {}, hashes: {}",
                                      last_confirmed_block, hashes[:gateway_constants.LOGGING_LIMIT_ITEM_COUNT])
        return self.block_cleanup_poll_interval_s

    @abstractmethod
    def _build_get_blocks_message_for_block_confirmation(self, hashes: List[Sha256Hash]) -> AbstractMessage:
        pass

    def publish_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview
    ) -> None:
        pass
