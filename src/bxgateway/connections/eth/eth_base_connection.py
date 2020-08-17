from typing import TYPE_CHECKING

from bxcommon.messages.abstract_message import AbstractMessage
from bxgateway.messages.eth.protocol.ping_eth_protocol_message import PingEthProtocolMessage
from bxutils import logging

from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)


class EthBaseConnection(AbstractGatewayBlockchainConnection["EthGatewayNode"]):
    def ping_message(self) -> AbstractMessage:
        return PingEthProtocolMessage(None)

    def enqueue_msg(self, msg, prepend=False):
        if not self.is_alive():
            return

        self._log_message(msg.log_level(), "Enqueued message: {}", msg)

        full_message_bytes = bytearray()
        for message_bytes in self.connection_protocol.get_message_bytes(msg):
            full_message_bytes.extend(message_bytes)

        self.enqueue_msg_bytes(full_message_bytes, prepend, full_message=msg)
