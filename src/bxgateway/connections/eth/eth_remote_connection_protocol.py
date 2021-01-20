from typing import Union, List

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.messages.abstract_block_message import AbstractBlockMessage

from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import GetBlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.receipts_eth_protocol_message import ReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message_v63 import StatusEthProtocolMessageV63
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxutils import logging

logger = logging.get_logger(__name__)


class EthRemoteConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, rlpx_cipher: RLPxCipher):
        super().__init__(connection, is_handshake_initiator, rlpx_cipher)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.GET_BLOCK_BODIES: self.msg_get_block_bodies,
            EthProtocolMessageType.BLOCK_HEADERS: self.msg_proxy_response,
            EthProtocolMessageType.BLOCK_BODIES: self.msg_block_bodies,
            EthProtocolMessageType.NODE_DATA: self.msg_proxy_response,
            EthProtocolMessageType.RECEIPTS: self.msg_block_receipts
        })

    def msg_status(self, msg: Union[StatusEthProtocolMessage, StatusEthProtocolMessageV63]):
        super(EthRemoteConnectionProtocol, self).msg_status(msg)
        self.connection.on_connection_established()
        self.node.remote_blockchain_connection_established = True
        self.node.on_remote_blockchain_connection_ready(self.connection)

        self.connection.schedule_pings()

    def msg_block_bodies(self, msg: BlockBodiesEthProtocolMessage) -> None:
        self.node.log_received_remote_blocks(len(msg.get_block_bodies_bytes()))
        self.msg_proxy_response(msg)

    def msg_block_receipts(self, msg: ReceiptsEthProtocolMessage) -> None:
        self.node.log_received_remote_blocks(len(msg.get_receipts_bytes()))
        self.msg_proxy_response(msg)

    def msg_get_block_bodies(self, msg: GetBlockBodiesEthProtocolMessage) -> None:
        block_hashes = msg.get_block_hashes()
        logger.debug(
            "Received unexpected get block bodies message from remote "
            "blockchain node for {} blocks: {}. Replying with empty block bodies.",
            len(block_hashes),
            ", ".join(
                [convert.bytes_to_hex(block_hash.binary) for block_hash in block_hashes[:10]]
            )
        )
        empty_block_bodies_msg = BlockBodiesEthProtocolMessage(None, [])
        self.connection.enqueue_msg(empty_block_bodies_msg)

    def msg_block(self, msg: AbstractBlockMessage) -> None:
        raise NotImplemented

    def _build_get_blocks_message_for_block_confirmation(
        self, hashes: List[Sha256Hash]
    ) -> AbstractMessage:
        pass

    def _set_transaction_contents(
        self, tx_hash: Sha256Hash, tx_content: Union[memoryview, bytearray]
    ) -> None:
        pass

