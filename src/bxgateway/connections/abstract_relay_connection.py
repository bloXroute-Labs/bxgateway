import time
from abc import ABCMeta
from typing import TYPE_CHECKING

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.messages.bloxroute.abstract_cleanup_message import AbstractCleanupMessage
from bxcommon.messages.bloxroute.bdn_performance_stats_message import BdnPerformanceStatsMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.bloxroute_message_validator import BloxrouteMessageValidator
from bxcommon.messages.bloxroute.disconnect_relay_peer_message import DisconnectRelayPeerMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.notification_message import NotificationMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.messages.validation.message_size_validation_settings import MessageSizeValidationSettings
from bxcommon.models.entity_type_model import EntityType
from bxcommon.models.notification_code import NotificationCode
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.utils import convert, performance_utils
from bxcommon.utils import memory_utils
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import hooks, stats_format
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import log_messages, gateway_constants
from bxgateway.feed.new_transaction_feed import NewTransactionFeed, \
    TransactionFeedEntry
from bxgateway.utils.logging.status import status_log
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service, \
    GatewayBdnPerformanceStatInterval
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service
from bxutils import logging
from bxutils.logging import LogLevel
from bxutils.logging import LogRecordType

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

msg_handling_logger = logging.get_logger(LogRecordType.MessageHandlingTroubleshooting, __name__)


class AbstractRelayConnection(InternalNodeConnection["AbstractGatewayNode"]):
    __metaclass__ = ABCMeta

    CONNECTION_TYPE = ConnectionType.RELAY_ALL

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(AbstractRelayConnection, self).__init__(sock, node)

        hello_msg = HelloMessage(protocol_version=self.protocol_version, network_num=self.network_num,
                                 node_id=self.node.opts.node_id)
        self.enqueue_msg(hello_msg)

        self.hello_messages = constants.BLOXROUTE_HELLO_MESSAGES
        self.header_size = constants.STARTING_SEQUENCE_BYTES_LEN + constants.BX_HDR_COMMON_OFF
        self.message_handlers = {
            BloxrouteMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.PING: self.msg_ping,
            BloxrouteMessageType.PONG: self.msg_pong,
            BloxrouteMessageType.ACK: self.msg_ack,
            BloxrouteMessageType.BROADCAST: self.msg_broadcast,
            BloxrouteMessageType.KEY: self.msg_key,
            BloxrouteMessageType.TRANSACTION: self.msg_tx,
            BloxrouteMessageType.TRANSACTIONS: self.msg_txs,
            BloxrouteMessageType.BLOCK_HOLDING: self.msg_block_holding,
            BloxrouteMessageType.DISCONNECT_RELAY_PEER: self.msg_disconnect_relay_peer,
            BloxrouteMessageType.TX_SERVICE_SYNC_TXS: self.msg_tx_service_sync_txs,
            BloxrouteMessageType.TX_SERVICE_SYNC_COMPLETE: self.msg_tx_service_sync_complete,
            BloxrouteMessageType.BLOCK_CONFIRMATION: self.msg_cleanup,
            BloxrouteMessageType.TRANSACTION_CLEANUP: self.msg_cleanup,
            BloxrouteMessageType.NOTIFICATION: self.msg_notify,
        }

        msg_size_validation_settings = MessageSizeValidationSettings(self.node.network.max_block_size_bytes,
                                                                     self.node.network.max_tx_size_bytes)
        self.message_validator = BloxrouteMessageValidator(msg_size_validation_settings, self.protocol_version)

    def msg_hello(self, msg):
        super(AbstractRelayConnection, self).msg_hello(msg)
        self.node.on_relay_connection_ready()

    def msg_broadcast(self, msg):
        """
        Handle broadcast message receive from bloXroute.
        This is typically an encrypted block.
        """
        if ConnectionType.RELAY_BLOCK in self.CONNECTION_TYPE:
            self.node.block_processing_service.process_block_broadcast(msg, self)
        else:
            self.log_error(log_messages.UNEXPECTED_BLOCK_ON_NON_RELAY_CONN, msg)

    def msg_key(self, msg):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        if ConnectionType.RELAY_BLOCK in self.CONNECTION_TYPE:
            self.node.block_processing_service.process_block_key(msg, self)
        else:
            self.log_error(log_messages.UNEXPECTED_KEY_MESSAGE, msg)

    def msg_tx(self, msg):
        """
        Handle transactions receive from bloXroute network.
        """

        start_time = time.time()

        if ConnectionType.RELAY_TRANSACTION not in self.CONNECTION_TYPE:
            self.log_error(log_messages.UNEXPECTED_TX_MESSAGE, msg)
            return

        tx_service = self.node.get_tx_service()

        tx_hash = msg.tx_hash()
        short_id = msg.short_id()
        tx_contents = msg.tx_val()
        is_compact = msg.is_compact()
        network_num = msg.network_num()
        attempt_recovery = False

        ext_start_time = time.time()

        processing_result = tx_service.process_gateway_transaction_from_bdn(tx_hash, short_id, tx_contents, is_compact)

        ext_end_time = time.time()

        if processing_result.ignore_seen:
            gateway_transaction_stats_service.log_duplicate_transaction_from_relay()
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER_IGNORE_SEEN,
                network_num,
                short_id,
                peer=stats_format.connection(self),
                is_compact_transaction=False
            )
            self.log_trace("Transaction has already been seen: {}", tx_hash)
            return

        if processing_result.existing_short_id:
            gateway_transaction_stats_service.log_duplicate_transaction_from_relay(is_compact)
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER_IGNORE_SEEN,
                network_num,
                short_id,
                peer=stats_format.connection(self),
                is_compact_transaction=is_compact
            )
            return

        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER,
            network_num,
            short_id,
            peer=stats_format.connection(self),
            is_compact_transaction=msg.is_compact()
        )
        gateway_transaction_stats_service.log_transaction_from_relay(
            tx_hash,
            short_id is not None,
            msg.is_compact()
        )

        if processing_result.assigned_short_id:
            was_missing = self.node.block_recovery_service.check_missing_sid(short_id)
            attempt_recovery |= was_missing
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_SHORT_ID_STORED_BY_GATEWAY,
                network_num,
                short_id,
                was_missing=was_missing
            )
            gateway_transaction_stats_service.log_short_id_assignment_processed()
        elif not short_id:
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_SHORT_ID_EMPTY_IN_MSG_FROM_RELAY,
                network_num,
                short_id,
                peer=stats_format.connection(self)
            )

        if not is_compact and processing_result.existing_contents:
            gateway_transaction_stats_service.log_redundant_transaction_content()

        if processing_result.set_content:
            self.log_trace("Adding hash value to tx service and forwarding it to node")
            gateway_bdn_performance_stats_service.log_tx_from_bdn()
            attempt_recovery |= self.node.block_recovery_service.check_missing_tx_hash(tx_hash)

            if self.node.node_conn is not None:
                blockchain_tx_message = self.node.message_converter.bx_tx_to_tx(msg)
                self.publish_new_transaction(
                    tx_hash, tx_contents
                )
                transaction_feed_stats_service.log_new_transaction(tx_hash)
                self.node.send_msg_to_node(blockchain_tx_message)

                tx_stats.add_tx_by_hash_event(
                    tx_hash,
                    TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_BLOCKCHAIN_NODE,
                    network_num,
                    short_id
                )

        if attempt_recovery:
            self.node.block_processing_service.retry_broadcast_recovered_blocks(self)

        end_time = time.time()

        total_duration_ms = (end_time - start_time) * 1000
        duration_before_ext_ms = (ext_start_time - start_time) * 1000
        duration_ext_ms = (ext_end_time - ext_start_time) * 1000
        duration_after_ext_ms = (end_time - ext_end_time) * 1000

        gateway_transaction_stats_service.log_processed_bdn_transaction(
            total_duration_ms,
            duration_before_ext_ms,
            duration_ext_ms,
            duration_after_ext_ms
        )

        performance_utils.log_operation_duration(
            msg_handling_logger,
            "Process single transaction from BDN",
            start_time,
            gateway_constants.BDN_TX_PROCESSING_TIME_WARNING_THRESHOLD_S,
            connection=self,
            message=msg,
            total_duration_ms=total_duration_ms,
            duration_before_ext_ms=duration_before_ext_ms,
            duration_ext_ms=duration_ext_ms,
            duration_after_ext_ms=duration_after_ext_ms
        )

    def msg_txs(self, msg: TxsMessage):
        if ConnectionType.RELAY_TRANSACTION not in self.CONNECTION_TYPE:
            self.log_error(log_messages.UNEXPECTED_TXS_ON_NON_RELAY_CONN, msg)
            return

        transactions = msg.get_txs()
        tx_service = self.node.get_tx_service()

        tx_stats.add_txs_by_short_ids_event(
            map(lambda x: x.short_id, transactions),
            TransactionStatEventType.TX_UNKNOWN_SHORT_IDS_REPLY_RECEIVED_BY_GATEWAY_FROM_RELAY,
            network_num=self.node.network_num,
            peer=stats_format.connection(self),
            found_tx_hashes=map(lambda x: convert.bytes_to_hex(x.hash.binary),
                                transactions
                                )
        )

        for transaction in transactions:
            tx_hash, transaction_contents, short_id = transaction

            assert tx_hash is not None
            assert short_id is not None

            self.node.block_recovery_service.check_missing_sid(short_id)

            if not tx_service.has_short_id(short_id):
                tx_service.assign_short_id(tx_hash, short_id)

            self.node.block_recovery_service.check_missing_tx_hash(tx_hash)

            if not tx_service.has_transaction_contents(tx_hash):
                assert transaction_contents is not None
                tx_service.set_transaction_contents(tx_hash, transaction_contents)

            tx_stats.add_tx_by_hash_event(tx_hash,
                                          TransactionStatEventType.TX_UNKNOWN_TRANSACTION_RECEIVED_BY_GATEWAY_FROM_RELAY,
                                          self.node.network_num, short_id, peer=stats_format.connection(self))

        self.node.block_processing_service.retry_broadcast_recovered_blocks(self)

        for block_awaiting_recovery in self.node.block_recovery_service.get_blocks_awaiting_recovery():
            self.node.block_processing_service.schedule_recovery_retry(block_awaiting_recovery)

    def msg_block_holding(self, msg):
        """
        Block holding request message handler. Places block on hold and broadcasts message to relay and gateway peers.
        :param msg: Message of type BlockHoldingMessage
        """
        block_hash = msg.block_hash()
        self.node.block_processing_service.place_hold(block_hash, self)

    def msg_disconnect_relay_peer(self, _msg: DisconnectRelayPeerMessage) -> None:
        """
        Drop relay peer request handler. Forces a gateway to drop its relay connection and request a new one
        :return: None
        """
        self.log_info("Received disconnect request. Dropping.")
        self.mark_for_close(should_retry=False)

    def log_connection_mem_stats(self) -> None:
        """
        logs the connection's memory stats
        """
        super(AbstractRelayConnection, self).log_connection_mem_stats()

        class_name = self.__class__.__name__
        if self.node.message_converter is not None:
            hooks.add_obj_mem_stats(
                class_name,
                self.network_num,
                self.node.message_converter,
                "message_converter",
                memory_utils.ObjectSize(
                    "message_converter", memory_utils.get_special_size(self.node.message_converter).size,
                    is_actual_size=True
                ),
                object_item_count=1,
                object_type=memory_utils.ObjectType.META,
                size_type=memory_utils.SizeType.SPECIAL
            )

    def send_bdn_performance_stats(self, bdn_stats_interval: GatewayBdnPerformanceStatInterval):
        msg_to_send = BdnPerformanceStatsMessage(
            bdn_stats_interval.start_time,
            bdn_stats_interval.end_time,
            bdn_stats_interval.new_blocks_received_from_blockchain_node,
            bdn_stats_interval.new_blocks_received_from_bdn,
            bdn_stats_interval.new_tx_received_from_blockchain_node,
            bdn_stats_interval.new_tx_received_from_bdn
        )
        self.enqueue_msg(msg_to_send)

    def msg_cleanup(self, msg: AbstractCleanupMessage):
        self.node.block_cleanup_service.process_cleanup_message(msg, self.node)

    def msg_notify(self, msg: NotificationMessage) -> None:
        if msg.notification_code() == NotificationCode.QUOTA_FILL_STATUS:
            seconds_since_last_quota_notification = time.time() - self.node.last_quota_level_notification_time
            if seconds_since_last_quota_notification < gateway_constants.QUOTA_NOTIFICATION_IGNORE_REPEAT_WINDOW_S:
                return

            self.node.last_quota_level_notification_time = time.time()
            args_list = msg.raw_message().split(",")
            entity_type = EntityType(int(args_list[1]))
            quota_level = int(args_list[0])
            if entity_type == EntityType.TRANSACTION and self.node.quota_level != quota_level:
                self.node.quota_level = quota_level
                self.node.alarm_queue.register_approx_alarm(
                    2 * constants.MIN_SLEEP_TIMEOUT, constants.MIN_SLEEP_TIMEOUT, status_log.update_alarm_callback,
                    self.node.connection_pool, self.node.opts.use_extensions, self.node.opts.source_version,
                    self.node.opts.external_ip, self.node.opts.continent, self.node.opts.country,
                    self.node.opts.should_update_source_version, self.node.account_id,
                    self.node.quota_level
                )

        if msg.level() == LogLevel.WARNING or msg.level() == LogLevel.ERROR:
            self.log(msg.level(), log_messages.NOTIFICATION_FROM_RELAY, msg.formatted_message())
        else:
            self.log(msg.level(), "Notification from Relay: {}", msg.formatted_message())

    def publish_new_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview
    ) -> None:
        self.node.feed_manager.publish_to_feed(
            NewTransactionFeed.NAME,
            TransactionFeedEntry(tx_hash, tx_contents)
        )
