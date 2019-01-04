from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection


class EthBaseConnection(AbstractGatewayBlockchainConnection):
    def enqueue_msg(self, msg, prepend=False):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        if msg.should_log_debug():
            logger.debug("Enqueued message: {} on connection: {}".format(msg, self))
        else:
            logger.info("Enqueued message: {} on connection: {}".format(msg, self))

        for message_bytes in self.connection_protocol.get_message_bytes(msg):
            self.enqueue_msg_bytes(message_bytes, prepend)
