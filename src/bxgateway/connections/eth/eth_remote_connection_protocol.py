from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class EthRemoteConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthRemoteConnectionProtocol, self).__init__(connection, is_handshake_initiator, private_key, public_key)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.BLOCK_HEADERS: self.msg_proxy_response,
            EthProtocolMessageType.BLOCK_BODIES: self.msg_proxy_response,
        })

    def msg_status(self, msg):
        logger.debug("Status message received.")

        self.connection.state |= ConnectionState.ESTABLISHED
        self.connection.node.remote_node_conn = self

        self.connection.send_ping()
