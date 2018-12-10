import random

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.utils import logger
from bxgateway import gateway_constants
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_factory import gateway_message_factory
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxgateway.messages.gateway.gateway_version_manager import gateway_version_manager


class GatewayConnection(InternalNodeConnection):
    """
    Connection handler for Gateway-Gateway connections. TODO: GatewayPeerConnection?
    """

    connection_type = ConnectionType.GATEWAY

    NULL_ORDERING = -1
    ACK_MESSAGE = AckMessage()

    def __init__(self, sock, address, node, from_me=False):
        super(GatewayConnection, self).__init__(sock, address, node, from_me)

        self.hello_messages = gateway_constants.GATEWAY_HELLO_MESSAGES
        self.header_size = constants.HDR_COMMON_OFF

        self.message_factory = gateway_message_factory
        self.message_handlers = {
            GatewayMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.ACK: self.msg_ack,
        }
        self.version_manager = gateway_version_manager
        self.protocol_version = self.version_manager.CURRENT_PROTOCOL_VERSION

        if from_me:
            self._initialize_ordered_handshake()
        else:
            self.ordering = self.NULL_ORDERING

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
            logger.error("Network number mismatch. Current network num {}, remote network num {}. Closing connection."
                         .format(self.network_num, network_num))
            self.mark_for_close()
            return

        ip = msg.ip()
        if ip != self.peer_ip:
            logger.error("Received hello message with mismatched IP address. Disconnecting.")
            self.mark_for_close()
            return

        port = msg.port()
        ordering = msg.ordering()

        if self.node.connection_exists(ip, port):
            connection = self.node.connection_pool.get_byipport(ip, port)

            # connection already has correct ip / port info assigned
            if connection == self:
                logger.warn("Duplicate hello message received. Ignoring.")
                self.enqueue_msg(self.ack_message)
                return

            if connection.state & ConnectionState.ESTABLISHED == ConnectionState.ESTABLISHED:
                logger.warn("Duplicate established connection. Dropping.")
                self.mark_for_close()
                return

            # ordering numbers were the same; take over the slot and try again
            if connection.ordering == ordering:
                logger.warn("Duplicate connection orderings could not be resolved. Investigate if this message appears "
                            "repeatedly.")
                self.node.connection_pool.update_port(port, self)
                self._initialize_ordered_handshake()
                return

            # part one of duplicate connection resolution
            # if connection has same ordering or no ordering, it hasn't yet received its connection
            # so don't do anything and wait for the other connection to receive a HELLO message and itself or this one
            if connection.ordering == self.NULL_ORDERING:
                logger.warn("Duplicate connections. Two connections have been opened on ip port: {}:{}. Awaiting other "
                            "connection's resolution.".format(ip, port))
                self.ordering = ordering
                return

            if connection.ordering > ordering:
                logger.debug("Connection already exists, with higher priority. Dropping connection {} and keeping {}."
                             .format(self.fileno, connection.fileno))
                self.mark_for_close()
                connection.state |= ConnectionState.ESTABLISHED
                connection.enqueue_msg(connection.ack_message)
            else:
                logger.debug("Connection already exists, with lower priority. Dropping connection {} and keeping {}."
                             .format(connection.fileno, self.fileno))
                connection.mark_for_close()
                self.state |= ConnectionState.ESTABLISHED
                self.enqueue_msg(self.ack_message)
                self.node.connection_pool.update_port(port, self)
        else:
            logger.debug("Connection is only one of its kind. Updating port reference.")
            self.node.connection_pool.update_port(port, self)
            self.enqueue_msg(self.ack_message)

        self._update_port_info(port)
        self.ordering = ordering

    def msg_ack(self, _msg):
        logger.debug("Received ack. Initializing connection.")
        self.state |= ConnectionState.ESTABLISHED

    def _initialize_ordered_handshake(self):
        self.ordering = random.getrandbits(constants.UL_INT_SIZE_IN_BYTES * 8)
        self.enqueue_msg(GatewayHelloMessage(self.protocol_version, self.network_num, self.node.opts.external_ip,
                                             self.node.opts.external_port, self.ordering))

    def _update_port_info(self, new_port):
        self.peer_port = new_port
        self.peer_desc = "%s %d" % (self.peer_ip, self.peer_port)
