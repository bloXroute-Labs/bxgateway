from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class EthNodeConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthNodeConnectionProtocol, self).__init__(connection, is_handshake_initiator, private_key, public_key)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.TRANSACTIONS: self.msg_tx,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_proxy_request,
            EthProtocolMessageType.GET_BLOCK_BODIES: self.msg_proxy_request,
            EthProtocolMessageType.NEW_BLOCK: self.msg_block,
        })

    def msg_status(self, msg):
        logger.debug("Status message received.")

        self.connection.state |= ConnectionState.ESTABLISHED
        self.connection.node.node_conn = self.connection

        self.connection.send_ping()
