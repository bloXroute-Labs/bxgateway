import random

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.internal_node_connection import InternalNodeConnection
from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.utils import logger
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_factory import gateway_message_factory
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class GatewayConnection(InternalNodeConnection):
    """
    Connection handler for Gateway-Gateway connections. TODO: GatewayPeerConnection?
    """

    connection_type = ConnectionType.GATEWAY

    NULL_ORDERING = -1
    ACK_MESSAGE = AckMessage()

    def __init__(self, sock, address, node, from_me=False):
        super(GatewayConnection, self).__init__(sock, address, node, from_me)

        self.hello_messages = [GatewayMessageType.HELLO]
        self.header_size = constants.HDR_COMMON_OFF
        self.message_factory = gateway_message_factory
        self.message_handlers = {
            GatewayMessageType.HELLO: self.msg_hello,
            BloxrouteMessageType.ACK: self.msg_ack,
        }
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
        ip = msg.ip()
        if ip != self.peer_ip:
            logger.error("Received hello message with mismatched IP address. Disconnecting.")
            self.node.enqueue_disconnect(self.fileno)

        port = msg.port()
        ordering = msg.ordering()
        self.ordering = ordering

        if self.node.connection_exists(ip, port):
            connection = self.node.connection_pool.get_byipport(ip, port)

            # connection already has correct ip / port info assigned
            if connection == self:
                return

            # ordering numbers were the same; take over the slot and try again
            if connection.ordering == ordering:
                logger.warn("Duplicate connection orderings could not be resolved. Investigate if this message appears"
                            "repeatedly.")
                self.node.connection_pool.byipport[(ip, port)] = self
                self._initialize_ordered_handshake()
                return

            if connection.ordering > ordering:
                logger.debug("Connection already exists, with higher priority. Dropping connection {} and keeping {}."
                             .format(self.fileno, connection.fileno))
                self.node.enqueue_disconnect(self.fileno)
                connection.state |= ConnectionState.ESTABLISHED
            else:
                logger.debug("Connection already exists, with lower priority. Dropping connection {} and keeping {}."
                             .format(connection.fileno, self.fileno))
                self.node.enqueue_disconnect(connection.fileno)
                self.state |= ConnectionState.ESTABLISHED
                self.node.connection_pool.byipport[(ip, port)] = self
        else:
            logger.debug("Connection is first of its kind. Updating port reference.")
            self.node.connection_pool.byipport[(ip, port)] = self

        self._update_port_info(port)
        self.enqueue_msg(self.ack_message)

    def msg_ack(self, _msg):
        logger.debug("Received ack. Initializing connection.")
        self.state |= ConnectionState.ESTABLISHED

    def _initialize_ordered_handshake(self):
        self.ordering = random.getrandbits(constants.UL_INT_SIZE_IN_BYTES * 8)
        self.enqueue_msg(GatewayHelloMessage(self.node.opts.external_ip, self.node.opts.external_port, self.ordering))

    def _update_port_info(self, new_port):
        self.peer_port = new_port
        self.peer_desc = "%s %d" % (self.peer_ip, self.peer_port)
