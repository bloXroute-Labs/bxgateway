from typing import TYPE_CHECKING

from bxutils import logging

from bxcommon.connections.connection_state import ConnectionState

from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)


class EthBaseConnection(AbstractGatewayBlockchainConnection["EthGatewayNode"]):
    def enqueue_msg(self, msg, prepend=False):
        if not self.is_alive():
            return

        self._log_message(msg.log_level(), "Enqueued message: {}", msg)

        full_message_bytes = bytearray()
        for message_bytes in self.connection_protocol.get_message_bytes(msg):
            full_message_bytes.extend(message_bytes)

        self.enqueue_msg_bytes(full_message_bytes, prepend, full_message=msg)
