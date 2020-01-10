from bxutils import logging

from bxcommon.connections.connection_state import ConnectionState
from bxgateway import eth_constants
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.receipts_eth_protocol_message import ReceiptsEthProtocolMessage

logger = logging.get_logger(__name__)


class EthRemoteConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthRemoteConnectionProtocol, self).__init__(connection, is_handshake_initiator, private_key, public_key)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.BLOCK_HEADERS: self.msg_block_headers,
            EthProtocolMessageType.BLOCK_BODIES: self.msg_block_bodies,
            EthProtocolMessageType.NODE_DATA: self.msg_proxy_response,
            EthProtocolMessageType.RECEIPTS: self.msg_block_receipts
        })

    def msg_status(self, _msg):
        self.connection.on_connection_established()
        self.node.on_remote_blockchain_connection_ready(self.connection)

        self.connection.send_ping()

    def msg_block_headers(self, msg: BlockHeadersEthProtocolMessage) -> None:
        raw_headers = msg.get_block_headers()
        headers_list = list(raw_headers)

        if headers_list:
            get_msg, attempts = self.node.requested_remote_headers_queue.popleft()
            logger.debug(
                "Received non-empty headers message from this request: {}. We received {} from remote blockchain node "
                "after {} attempts. Forwarding.",
                get_msg,
                msg,
                attempts)
            self.msg_proxy_response(msg)

        else:
            # Remote blockchain sent empty headers message
            # In this case, do not forward to blockchain node
            logger.debug("Received empty headers message from remote blockchain node. Not forwarding.")
            get_msg, attempts = self.node.requested_remote_headers_queue.popleft()
            attempts += 1
            if attempts <= eth_constants.MAX_HEADERS_RESEND_ATTEMPTS:
                self.connection.node.alarm_queue.register_alarm(eth_constants.HEADERS_RETRY_INTERVAL_S,
                                                                self.get_headers_helper, get_msg, attempts)
            else:
                logger.debug("Received max empty headers messages from remote blockchain node for {}. Forwarding "
                             "empty message anyway.", get_msg)
                self.msg_proxy_response(msg)

    def msg_block_bodies(self, msg: BlockBodiesEthProtocolMessage) -> None:
        self.node.log_received_remote_blocks(len(msg.get_block_bodies_bytes()))
        self.msg_proxy_response(msg)

    def msg_block_receipts(self, msg: ReceiptsEthProtocolMessage) -> None:
        self.node.log_received_remote_blocks(len(msg.get_receipts_bytes()))
        self.msg_proxy_response(msg)

    def get_headers_helper(self, get_msg: GetBlockHeadersEthProtocolMessage, attempts: int) -> None:
        self.node.requested_remote_headers_queue.append((get_msg, attempts))
        logger.debug("Resending {} after {} attempts. Forwarding to remote blockchain node.", get_msg, attempts)
        self.msg_proxy_request(get_msg)
