import random
from typing import TYPE_CHECKING, cast, Optional

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.feed.feed import FeedKey
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.services import sdn_http_service
from bxcommon.utils import crypto
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.feed.feed_source import FeedSource
from bxgateway import gateway_constants
from bxgateway import log_messages
from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.messages.gateway.confirmed_tx_message import ConfirmedTxMessage
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_factory import gateway_message_factory
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxgateway.messages.gateway.gateway_version_manager import gateway_version_manager
from bxgateway.messages.gateway.request_tx_stream_message import RequestTxStreamMessage
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayConnection(InternalNodeConnection["AbstractGatewayNode"]):
    """
    Connection handler for Gateway-Gateway connections.
    """

    CONNECTION_TYPE = ConnectionType.EXTERNAL_GATEWAY

    NULL_ORDERING = -1
    ACK_MESSAGE = AckMessage()

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(GatewayConnection, self).__init__(sock, node)

        self.hello_messages = gateway_constants.GATEWAY_HELLO_MESSAGES
        self.header_size = constants.STARTING_SEQUENCE_BYTES_LEN + constants.BX_HDR_COMMON_OFF
        self.message_handlers = {
            GatewayMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.ACK: self.msg_ack,
            GatewayMessageType.BLOCK_RECEIVED: self.msg_block_received,
            GatewayMessageType.BLOCK_PROPAGATION_REQUEST: self.msg_block_propagation_request,
            BloxrouteMessageType.BLOCK_HOLDING: self.msg_block_holding,
            BloxrouteMessageType.KEY: self.msg_key,
            GatewayMessageType.CONFIRMED_TX: self.msg_confirmed_tx,
            GatewayMessageType.REQUEST_TX_STREAM: self.msg_request_tx_stream,
        }
        self.version_manager = gateway_version_manager
        self.protocol_version = self.version_manager.CURRENT_PROTOCOL_VERSION

        if self.from_me:
            self._initialize_ordered_handshake()
        else:
            self.ordering = self.NULL_ORDERING

    def connection_message_factory(self) -> AbstractMessageFactory:
        return gateway_message_factory

    def on_connection_established(self):
        super().on_connection_established()

        peer_model = self.peer_model
        if (
            self.node.opts.request_remote_transaction_streaming
            and peer_model is not None
            and peer_model.is_transaction_streamer()
        ):
            self.enqueue_msg(RequestTxStreamMessage())

    def msg_hello(self, msg):
        """
        Updates connection pool references to this connection to match the port described in the hello message.
        Identifies any duplicate connections (i.e. from if both nodes connect to each other) and drops them
        based on the ordering of the messages.

        Connections may end up with two entries in the connection pool if they are initiated by the remote peer, i.e.
        one on the randomly assigned incoming socket and one on the actual provided address of the peer here.
        """
        network_num = msg.network_num()
        if network_num != self.node.network_num:
            self.log_error(log_messages.NETWORK_NUMBER_MISMATCH, self.network_num, network_num)
            self.mark_for_close(should_retry=False)
            return

        ip = msg.ip()
        if ip != self.peer_ip:
            self.log_warning(log_messages.MISMATCH_IP_IN_HELLO_MSG, ip, self.peer_ip)

        port = msg.port()
        ordering = msg.ordering()
        peer_id = msg.node_id()
        if not self._is_authenticated:
            # naively set the the peer id to what reported
            if peer_id is None:
                self.log_warning(log_messages.HELLO_MSG_WITH_NO_PEER_ID)
            self.peer_id = peer_id

        if not self.from_me:
            self.log_trace("Connection established with peer: {}.", peer_id)
            self.node.requester.send_threaded_request(
                sdn_http_service.submit_gateway_inbound_connection,
                self.node.opts.node_id,
                self.peer_id
            )

        if self.node.connection_exists(ip, port):
            connection = self.node.connection_pool.get_by_ipport(ip, port)

            # connection already has correct ip / port info assigned
            if connection == self:
                self.log_debug("Duplicate hello message received. Ignoring.")
                self.enqueue_msg(self.ack_message)
                return

            if connection.is_active():
                self.log_debug("Duplicate established connection. Dropping.")
                self.mark_for_close(should_retry=False)
                return

            # ordering numbers were the same; take over the slot and try again
            if connection.ordering == ordering:
                self.log_debug("Duplicate connection orderings could not be resolved. Investigate if this "
                               "message appears repeatedly.")
                self.node.connection_pool.update_port(self.peer_port, port, self)
                self._initialize_ordered_handshake()
                return

            # part one of duplicate connection resolution
            # if connection has same ordering or no ordering, it hasn't yet received its connection
            # so don't do anything and wait for the other connection to receive a HELLO message and itself or this one
            if connection.ordering == self.NULL_ORDERING:
                self.log_debug(
                    "Duplicate connections. Two connections have been opened on ip port: {}:{}. Awaiting other "
                    "connection's resolution.", ip, port)
                self.ordering = ordering
                return

            if connection.ordering > ordering:
                self.log_warning(log_messages.REDUNDANT_CONNECTION, self.file_no, connection.file_no)
                self.mark_for_close(should_retry=False)
                connection.on_connection_established()
                connection.enqueue_msg(connection.ack_message)
            else:
                self.log_warning(log_messages.REDUNDANT_CONNECTION, connection.file_no, self.file_no)
                connection.mark_for_close(should_retry=False)
                self.on_connection_established()
                self.enqueue_msg(self.ack_message)
                self.node.connection_pool.update_port(self.peer_port, port, self)
        else:
            self.log_debug("Connection is only one of its kind. Updating port reference.")
            self.node.connection_pool.update_port(self.peer_port, port, self)
            self.enqueue_msg(self.ack_message)
            self.on_connection_established()

        self._update_port_info(port)
        self.ordering = ordering

    def msg_ack(self, _msg):
        """
        Acks hello message and establishes connection.
        """
        self.on_connection_established()

    def msg_block_received(self, msg):
        self.node.neutrality_service.record_block_receipt(msg.block_hash(), self)

    def msg_block_propagation_request(self, msg):
        bx_block = msg.blob()
        bx_block_hash = crypto.double_sha256(bx_block)
        block_stats.add_block_event_by_block_hash(
            bx_block_hash,
            BlockStatEventType.BX_BLOCK_PROPAGATION_REQUESTED_BY_PEER,
            network_num=self.network_num,
            peers=[self],
        )
        self.node.neutrality_service.propagate_block_to_network(bx_block, self, from_peer=True)

    def msg_block_holding(self, msg):
        block_hash = msg.block_hash()
        self.node.block_processing_service.place_hold(block_hash, self)

    def msg_key(self, msg):
        """
        Handles key message receive from bloXroute.
        Looks for the encrypted block and decrypts; otherwise stores for later.
        """
        self.node.block_processing_service.process_block_key(msg, self)

    def msg_confirmed_tx(self, msg: ConfirmedTxMessage) -> None:
        tx_hash = msg.tx_hash()
        transaction_key = self.node.get_tx_service().get_transaction_key(tx_hash)
        tx_contents = msg.tx_val()

        # shouldn't ever happen, but just in case
        if tx_contents == ConfirmedTxMessage.EMPTY_TX_VAL:
            tx_contents = cast(
                Optional[memoryview],
                self.node.get_tx_service().get_transaction_by_key(transaction_key)
            )
            if tx_contents is None:
                transaction_feed_stats_service.log_pending_transaction_missing_contents()
                return

        self.node.feed_manager.publish_to_feed(
            FeedKey(EthPendingTransactionFeed.NAME),
            EthRawTransaction(
                tx_hash, tx_contents, FeedSource.BDN_SOCKET, local_region=True
            )
        )

        transaction_feed_stats_service.log_pending_transaction_from_internal(tx_hash)

    def msg_request_tx_stream(self, msg: RequestTxStreamMessage) -> None:
        pass

    def _initialize_ordered_handshake(self):
        self.ordering = random.getrandbits(constants.UL_INT_SIZE_IN_BYTES * 8)
        self.enqueue_msg(GatewayHelloMessage(self.protocol_version, self.network_num, self.node.opts.external_ip,
                                             self.node.opts.external_port, self.ordering, self.node.opts.node_id))

    def _update_port_info(self, new_port):
        self.peer_port = new_port
        self.peer_desc = "%s %d" % (self.peer_ip, self.peer_port)

    def dispose(self):
        if not self.from_me:
            if self.peer_id:
                self.node.requester.send_threaded_request(
                    sdn_http_service.delete_gateway_inbound_connection,
                    self.node.opts.node_id,
                    self.peer_id
                )
        super().dispose()
