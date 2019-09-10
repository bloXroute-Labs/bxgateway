from bxutils import logging

from bxcommon.connections.connection_state import ConnectionState

from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection

logger = logging.get_logger(__name__)


class EthBaseConnection(AbstractGatewayBlockchainConnection):
    def enqueue_msg(self, msg, prepend=False):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        logger.log(msg.log_level(), "Enqueued message: {} on connection: {}".format(msg, self))

        full_message_bytes = bytearray()
        for message_bytes in self.connection_protocol.get_message_bytes(msg):
            full_message_bytes.extend(message_bytes)

        self.enqueue_msg_bytes(full_message_bytes, prepend, full_message=msg)
