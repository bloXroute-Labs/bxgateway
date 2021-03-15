import time
from abc import ABCMeta
from asyncio import Future
from typing import TYPE_CHECKING, Set

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.feed.feed import FeedKey
from bxcommon.messages.bloxroute.abstract_cleanup_message import AbstractCleanupMessage
from bxcommon.messages.bloxroute.bdn_performance_stats_message import BdnPerformanceStatsMessage
from bxcommon.messages.bloxroute.blockchain_network_message import RefreshBlockchainNetworkMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.bloxroute_message_validator import BloxrouteMessageValidator
from bxcommon.messages.bloxroute.compressed_block_txs_message import CompressedBlockTxsMessage
from bxcommon.messages.bloxroute.disconnect_relay_peer_message import DisconnectRelayPeerMessage
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.notification_message import NotificationMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.messages.validation.message_size_validation_settings import MessageSizeValidationSettings
from bxcommon.models.entity_type_model import EntityType
from bxcommon.models.notification_code import NotificationCode
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.services import sdn_http_service
from bxcommon.utils import convert, performance_utils
from bxcommon.utils import memory_utils
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import hooks, stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway import log_messages, gateway_constants
from bxcommon.feed.new_transaction_feed import NewTransactionFeed, RawTransactionFeedEntry
from bxgateway.services.block_recovery_service import RecoveredTxsSource
from bxgateway.services.gateway_transaction_service import MissingTransactions
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

    node: "AbstractGatewayNode"

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
            BloxrouteMessageType.COMPRESSED_BLOCK_TXS: self.msg_compressed_block_txs,
            BloxrouteMessageType.BLOCK_HOLDING: self.msg_block_holding,
            BloxrouteMessageType.DISCONNECT_RELAY_PEER: self.msg_disconnect_relay_peer,
            BloxrouteMessageType.TX_SERVICE_SYNC_TXS: self.tx_sync_service.msg_tx_service_sync_txs,
            BloxrouteMessageType.TX_SERVICE_SYNC_COMPLETE: self.tx_sync_service.msg_tx_service_sync_complete,
            BloxrouteMessageType.BLOCK_CONFIRMATION: self.msg_cleanup,
            BloxrouteMessageType.TRANSACTION_CLEANUP: self.msg_cleanup,
            BloxrouteMessageType.NOTIFICATION: self.msg_notify,
            BloxrouteMessageType.REFRESH_BLOCKCHAIN_NETWORK: self.msg_refresh_blockchain_network
        }

        msg_size_validation_settings = MessageSizeValidationSettings(self.node.network.max_block_size_bytes,
                                                                     self.node.network.max_tx_size_bytes)
        self.message_validator = BloxrouteMessageValidator(msg_size_validation_settings, self.protocol_version)

    def msg_hello(self, msg):
        super(AbstractRelayConnection, self).msg_hello(msg)
        self.node.on_relay_connection_ready(self.CONNECTION_TYPE)

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
                peers=[self],
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
                peers=[self],
                is_compact_transaction=is_compact
            )
            return

        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_RECEIVED_BY_GATEWAY_FROM_PEER,
            network_num,
            short_id,
            peers=[self],
            is_compact_transaction=msg.is_compact()
        )
        gateway_transaction_stats_service.log_transaction_from_relay(
            tx_hash,
            short_id is not None,
            msg.is_compact()
        )

        if processing_result.assigned_short_id:
            was_missing = self.node.block_recovery_service.check_missing_sid(
                short_id, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN
            )
            attempt_recovery |= was_missing
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_SHORT_ID_STORED_BY_GATEWAY,
                network_num,
                short_id,
                was_missing=was_missing
            )
            gateway_transaction_stats_service.log_short_id_assignment_processed()

        if not is_compact and processing_result.existing_contents:
            gateway_transaction_stats_service.log_redundant_transaction_content()

        if processing_result.set_content:
            self.log_trace("Adding hash value to tx service and forwarding it to node")
            gateway_bdn_performance_stats_service.log_tx_from_bdn(
                not self.node.is_gas_price_above_min_network_fee(tx_contents)
            )
            attempt_recovery |= self.node.block_recovery_service.check_missing_tx_hash(
                tx_hash, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN
            )

            self.publish_new_transaction(
                tx_hash, tx_contents, TransactionFlag.LOCAL_REGION in msg.transaction_flag()
            )

            if self.node.has_active_blockchain_peer():
                blockchain_tx_message = self.node.message_converter.bx_tx_to_tx(msg)
                transaction_feed_stats_service.log_new_transaction(tx_hash)

                sent = self.node.broadcast_transactions_to_nodes(blockchain_tx_message, self)
                if sent:
                    tx_stats.add_tx_by_hash_event(
                        tx_hash,
                        TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_BLOCKCHAIN_NODE,
                        network_num,
                        short_id
                    )
                    gateway_bdn_performance_stats_service.log_tx_sent_to_nodes()
                else:
                    gateway_transaction_stats_service.log_dropped_transaction_from_relay()

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

    def msg_txs(self, msg: TxsMessage, recovered_txs_source: RecoveredTxsSource = RecoveredTxsSource.TXS_RECOVERED):
        transactions = msg.get_txs()
        tx_service = self.node.get_tx_service()
        missing_txs: Set[MissingTransactions] = tx_service.process_txs_message(msg)

        tx_stats.add_txs_by_short_ids_event(
            map(lambda x: x.short_id, transactions),
            TransactionStatEventType.TX_UNKNOWN_SHORT_IDS_REPLY_RECEIVED_BY_GATEWAY_FROM_RELAY,
            network_num=self.node.network_num,
            peers=[self],
            found_tx_hashes=map(lambda x: convert.bytes_to_hex(x.hash.binary),
                                transactions
                                )
        )

        for (short_id, transaction_hash) in missing_txs:
            self.node.block_recovery_service.check_missing_sid(short_id, recovered_txs_source)
            self.node.block_recovery_service.check_missing_tx_hash(transaction_hash, recovered_txs_source)

            tx_stats.add_tx_by_hash_event(
                transaction_hash,
                TransactionStatEventType.TX_UNKNOWN_TRANSACTION_RECEIVED_BY_GATEWAY_FROM_RELAY,
                self.node.network_num,
                short_id,
                peers=[self]
            )

        self.node.block_processing_service.retry_broadcast_recovered_blocks(self)

        for block_awaiting_recovery in self.node.block_recovery_service.get_blocks_awaiting_recovery():
            self.node.block_processing_service.schedule_recovery_retry(block_awaiting_recovery)

    def msg_compressed_block_txs(self, msg: CompressedBlockTxsMessage):
        start_time = time.time()

        self.msg_txs(msg.to_txs_message(), RecoveredTxsSource.COMPRESSED_BLOCK_TXS_RECEIVED)

        block_stats.add_block_event_by_block_hash(
            msg.block_hash(),
            BlockStatEventType.ENC_BLOCK_COMPRESSED_TXS_RECEIVED_BY_GATEWAY,
            network_num=msg.network_num(),
            peers=[self],
            more_info="{}. processing time: {}".format(
                stats_format.connection(self),
                stats_format.timespan(start_time, time.time())
            )
        )

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
        if self.peer_ip in set([outbound_peer.ip for outbound_peer in self.node.opts.outbound_peers if outbound_peer]):
            self.log_info("Received disconnect request. Not dropping because relay peer is static.")
        else:
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
        memory_utilization_mb = int(memory_utils.get_app_memory_usage() / constants.BYTE_TO_MB)
        bdn_stats_per_node = bdn_stats_interval.blockchain_node_to_bdn_stats
        if bdn_stats_per_node is None:
            bdn_stats_per_node = {}

        msg_to_send = BdnPerformanceStatsMessage(
            bdn_stats_interval.start_time,
            bdn_stats_interval.end_time,
            memory_utilization_mb,
            bdn_stats_per_node
        )
        self.enqueue_msg(msg_to_send)

    def msg_cleanup(self, msg: AbstractCleanupMessage):
        self.node.block_cleanup_service.process_cleanup_message(msg, self.node)

    def msg_notify(self, msg: NotificationMessage) -> None:
        if msg.notification_code() == NotificationCode.QUOTA_FILL_STATUS:
            args_list = msg.raw_message().split(",")
            entity_type = EntityType(int(args_list[1]))
            quota_level = int(args_list[0])
            if entity_type == EntityType.TRANSACTION and self.node.quota_level != quota_level:
                self.node.quota_level = quota_level
                self.node.alarm_queue.register_approx_alarm(
                    2 * constants.MIN_SLEEP_TIMEOUT, constants.MIN_SLEEP_TIMEOUT, status_log.update_alarm_callback,
                    self.node.connection_pool, self.node.opts.use_extensions, self.node.opts.source_version,
                    self.node.opts.external_ip, self.node.opts.continent, self.node.opts.country,
                    self.node.opts.should_update_source_version, self.node.blockchain_peers, self.node.account_id,
                    self.node.quota_level
                )
        elif (
            msg.notification_code() == NotificationCode.ASSIGNING_SHORT_IDS
            or msg.notification_code() == NotificationCode.NOT_ASSIGNING_SHORT_IDS
        ):
            is_assigning = msg.notification_code() == NotificationCode.ASSIGNING_SHORT_IDS
            peer_model = self.peer_model
            if peer_model is not None:
                peer_model.assigning_short_ids = is_assigning
            if self.node.opts.split_relays:
                for peer_model in self.node.peer_transaction_relays:
                    if peer_model.ip == self.peer_ip and peer_model.node_id == self.peer_id:
                        peer_model.assigning_short_ids = is_assigning

        if msg.level() == LogLevel.WARNING or msg.level() == LogLevel.ERROR:
            self.log(msg.level(), log_messages.NOTIFICATION_FROM_RELAY, msg.formatted_message())
        else:
            self.log(msg.level(), "Notification from Relay: {}", msg.formatted_message())

    def msg_refresh_blockchain_network(self, _msg: RefreshBlockchainNetworkMessage) -> None:
        blockchain_protocol = self.node.opts.blockchain_protocol
        blockchain_network = self.node.opts.blockchain_network
        assert blockchain_protocol is not None
        assert blockchain_network is not None

        self.node.requester.send_threaded_request(
            sdn_http_service.fetch_blockchain_network,
            blockchain_protocol,
            blockchain_network,
            # pyre-fixme[6]: Expected `Optional[Callable[[Future[Any]], Any]]` for 4th parameter `done_callback`
            #  to call `send_threaded_request` but got `BoundMethod[Callable(_process_blockchain_network_from_sdn)
            #  [[Named(self, AbstractRelayConnection), Named(get_blockchain_network_future, Future[Any])], Any],
            #  AbstractRelayConnection]`.
            done_callback=self._process_blockchain_network_from_sdn
        )

    def publish_new_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview, local_region: bool
    ) -> None:
        self.node.feed_manager.publish_to_feed(
            FeedKey(NewTransactionFeed.NAME),
            RawTransactionFeedEntry(tx_hash, tx_contents, local_region=local_region)
        )

    def on_connection_established(self):
        super(AbstractRelayConnection, self).on_connection_established()

        if self.is_relay_connection() and self.node.opts.split_relays:
            self.node.alarm_queue.register_alarm(gateway_constants.CHECK_RELAY_CONNECTIONS_DELAY_S,
                                                 self._check_matching_relay_connection)

    def _check_matching_relay_connection(self):
        self.log_debug("Verifying that matching relay connection has been established.")

        matching_relay_ip = self.peer_ip
        if self.CONNECTION_TYPE == ConnectionType.RELAY_TRANSACTION:
            matching_relay_port = self.peer_port - 1
        else:
            matching_relay_port = self.peer_port + 1

        if self.node.connection_pool.has_connection(matching_relay_ip, matching_relay_port):
            matching_connection = self.node.connection_pool.by_ipport[(matching_relay_ip, matching_relay_port)]
            if matching_connection.is_active():
                self.log_debug("Matching connection check for relay connection {} is successful. "
                               "Matching connection is {}", self, matching_connection)
                return

        self.log_debug("Closing relay connection {} because matching relay connection is not available.", self)
        if self.CONNECTION_TYPE == ConnectionType.RELAY_TRANSACTION:
            self.node.remove_relay_transaction_peer(self.peer_ip, self.peer_port, True)
        else:
            self.node.remove_relay_peer(self.peer_ip, self.peer_port)

    def _process_blockchain_network_from_sdn(self, get_blockchain_network_future: Future):
        try:
            updated_blockchain_network = get_blockchain_network_future.result()
            assert updated_blockchain_network is not None

            self.node.network = updated_blockchain_network
            self.node.opts.blockchain_networks[self.node.network_num] = updated_blockchain_network
            self.node.update_node_settings_from_blockchain_network(updated_blockchain_network)
        except Exception as e:
            self.log_info("Got {} when trying to read blockchain network from sdn", e)
