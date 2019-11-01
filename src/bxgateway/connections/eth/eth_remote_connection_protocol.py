from bxutils import logging

from bxcommon.connections.connection_state import ConnectionState

from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.receipts_eth_protocol_message import ReceiptsEthProtocolMessage

logger = logging.get_logger(__name__)


class EthRemoteConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthRemoteConnectionProtocol, self).__init__(connection, is_handshake_initiator, private_key, public_key)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.BLOCK_HEADERS: self.msg_proxy_response,
            EthProtocolMessageType.BLOCK_BODIES: self.msg_block_bodies,
            EthProtocolMessageType.NODE_DATA: self.msg_proxy_response,
            EthProtocolMessageType.RECEIPTS: self.msg_block_receipts
        })

    def msg_status(self, _msg):
        self.connection.on_connection_established()
        self.node.on_remote_blockchain_connection_ready(self.connection)

        self.connection.send_ping()

    def msg_block_bodies(self, msg: BlockBodiesEthProtocolMessage) -> None:
        self.node.log_received_remote_blocks(len(msg.get_block_bodies_bytes()))
        self.msg_proxy_response(msg)

    def msg_block_receipts(self, msg: ReceiptsEthProtocolMessage) -> None:
        self.node.log_received_remote_blocks(len(msg.get_receipts_bytes()))
        self.msg_proxy_response(msg)
