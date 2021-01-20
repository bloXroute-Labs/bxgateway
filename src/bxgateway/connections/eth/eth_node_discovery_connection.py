import socket
import time
from typing import TYPE_CHECKING

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.messages.eth.discovery.eth_discovery_message_factory import eth_discovery_message_factory
from bxgateway.messages.eth.discovery.eth_discovery_message_type import EthDiscoveryMessageType
from bxgateway.messages.eth.discovery.ping_eth_discovery_message import PingEthDiscoveryMessage
from bxutils import logging
from bxgateway import log_messages
from bxcommon.utils.blockchain_utils.eth import eth_common_constants

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)


class EthNodeDiscoveryConnection(AbstractGatewayBlockchainConnection["EthGatewayNode"]):
    """
    Discovery protocol connection with Ethereum node.
    This connection is used to obtain public key of Ethereum node from Ping message.
    """

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: "EthGatewayNode"):
        super(EthNodeDiscoveryConnection, self).__init__(sock, node)

        self.message_handlers = {
            EthDiscoveryMessageType.PING: self.msg_ping,
            EthDiscoveryMessageType.PONG: self.msg_pong
        }

        self.can_send_pings = True
        self.pong_message = None

        self._pong_received = False

        self.send_ping()
        self.node.alarm_queue.register_alarm(eth_common_constants.DISCOVERY_PONG_TIMEOUT_SEC, self._pong_timeout)

        self.hello_messages = [EthDiscoveryMessageType.PING, EthDiscoveryMessageType.PONG]

    def connection_message_factory(self) -> AbstractMessageFactory:
        return eth_discovery_message_factory

    def ping_message(self) -> AbstractMessage:
        return PingEthDiscoveryMessage(
            None,
            self.node.get_private_key(),
            eth_common_constants.P2P_PROTOCOL_VERSION,
            (self.external_ip, self.external_port, self.external_port),
            (socket.gethostbyname(self.peer_ip), self.peer_port, self.peer_port),
            int(time.time()) + eth_common_constants.PING_MSG_TTL_SEC
        )

    def msg_ping(self, msg):
        pass

    def msg_pong(self, msg):
        self.node.set_remote_public_key(self, msg.get_public_key())
        self._pong_received = True

    def _pong_timeout(self):
        if not self._pong_received:
            self.log_warning(log_messages.PONG_MESSAGE_TIMEOUT)
            self.mark_for_close()
