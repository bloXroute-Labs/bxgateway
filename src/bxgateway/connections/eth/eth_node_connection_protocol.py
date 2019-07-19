from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxgateway import eth_constants
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage


class EthNodeConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthNodeConnectionProtocol, self).__init__(connection, is_handshake_initiator, private_key, public_key)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.TRANSACTIONS: self.msg_tx,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_get_block_headers,
            EthProtocolMessageType.GET_BLOCK_BODIES: self.msg_proxy_request,
            EthProtocolMessageType.GET_NODE_DATA: self.msg_proxy_request,
            EthProtocolMessageType.GET_RECEIPTS: self.msg_proxy_request,
            EthProtocolMessageType.NEW_BLOCK: self.msg_block,
        })

        self.waiting_checkpoint_headers_request = True

    def msg_status(self, _msg):
        logger.debug("Status message received.")

        self.connection.state |= ConnectionState.ESTABLISHED
        self.connection.node.node_conn = self.connection

        self.connection.send_ping()

        self.connection.node.alarm_queue.register_alarm(eth_constants.CHECKPOINT_BLOCK_HEADERS_REQUEST_WAIT_TIME_S,
                                                        self._stop_waiting_checkpoint_headers_request)

    def msg_get_block_headers(self, msg: GetBlockHeadersEthProtocolMessage):
        if self.waiting_checkpoint_headers_request:
            super(EthNodeConnectionProtocol, self).msg_get_block_headers(msg)
        else:
            self.msg_proxy_request(msg)

    def _stop_waiting_checkpoint_headers_request(self):
        self.waiting_checkpoint_headers_request = False
