import time
from typing import Type

from bxcommon.exceptions import ParseError
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory, MessagePreview
from bxcommon.utils.buffers.input_buffer import InputBuffer
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxgateway import gateway_constants
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_bodies_v66_eth_protocol_message import \
    BlockBodiesV66EthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_v66_eth_protocol_message import \
    BlockHeadersV66EthProtocolMessage
from bxgateway.messages.eth.protocol.disconnect_eth_protocol_message import DisconnectEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import GetBlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_bodies_v66_eth_protocol_message import \
    GetBlockBodiesV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_v66_eth_protocol_message import \
    GetBlockHeadersV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_node_data_eth_protocol_message import GetNodeDataEthProtocolMessage
from bxgateway.messages.eth.protocol.get_node_data_v66_eth_protocol_message import \
    GetNodeDataV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_pooled_transactions_eth_protocol_message import \
    GetPooledTransactionsEthProtocolMessage
from bxgateway.messages.eth.protocol.get_pooled_transactions_v66_eth_protocol_message import \
    GetPooledTransactionsV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_eth_protocol_message import GetReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_v66_eth_protocol_message import \
    GetReceiptsV66EthProtocolMessage
from bxgateway.messages.eth.protocol.hello_eth_protocol_message import HelloEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import NewBlockHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.new_pooled_transaction_hashes_eth_protocol_message import \
    NewPooledTransactionHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.node_data_eth_protocol_message import NodeDataEthProtocolMessage
from bxgateway.messages.eth.protocol.node_data_v66_eth_protocol_message import \
    NodeDataV66EthProtocolMessage
from bxgateway.messages.eth.protocol.ping_eth_protocol_message import PingEthProtocolMessage
from bxgateway.messages.eth.protocol.pong_eth_protocol_message import PongEthProtocolMessage
from bxgateway.messages.eth.protocol.pooled_transactions_eth_protocol_message import \
    PooledTransactionsEthProtocolMessage
from bxgateway.messages.eth.protocol.pooled_transactions_v66_eth_protocol_message import \
    PooledTransactionsV66EthProtocolMessage
from bxgateway.messages.eth.protocol.raw_eth_protocol_message import RawEthProtocolMessage
from bxgateway.messages.eth.protocol.receipts_eth_protocol_message import ReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.receipts_v66_eth_protocol_message import \
    ReceiptsV66EthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message_v63 import StatusEthProtocolMessageV63
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.utils.eth.framed_input_buffer import FramedInputBuffer
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxgateway.utils.stats.eth.eth_gateway_stats_service import eth_gateway_stats_service


class EthProtocolMessageFactory(AbstractMessageFactory):
    _MESSAGE_TYPE_MAPPING = {
        EthProtocolMessageType.HELLO: HelloEthProtocolMessage,
        EthProtocolMessageType.DISCONNECT: DisconnectEthProtocolMessage,
        EthProtocolMessageType.PING: PingEthProtocolMessage,
        EthProtocolMessageType.PONG: PongEthProtocolMessage,
        EthProtocolMessageType.TRANSACTIONS: TransactionsEthProtocolMessage,
        EthProtocolMessageType.NEW_BLOCK_HASHES: NewBlockHashesEthProtocolMessage,
        EthProtocolMessageType.GET_BLOCK_HEADERS: GetBlockHeadersEthProtocolMessage,
        EthProtocolMessageType.BLOCK_HEADERS: BlockHeadersEthProtocolMessage,
        EthProtocolMessageType.GET_BLOCK_BODIES: GetBlockBodiesEthProtocolMessage,
        EthProtocolMessageType.BLOCK_BODIES: BlockBodiesEthProtocolMessage,
        EthProtocolMessageType.NEW_BLOCK: NewBlockEthProtocolMessage,
        EthProtocolMessageType.GET_NODE_DATA: GetNodeDataEthProtocolMessage,
        EthProtocolMessageType.NODE_DATA: NodeDataEthProtocolMessage,
        EthProtocolMessageType.GET_RECEIPTS: GetReceiptsEthProtocolMessage,
        EthProtocolMessageType.RECEIPTS: ReceiptsEthProtocolMessage,
        EthProtocolMessageType.NEW_POOLED_TRANSACTION_HASHES: NewPooledTransactionHashesEthProtocolMessage,
        EthProtocolMessageType.GET_POOLED_TRANSACTIONS: GetPooledTransactionsEthProtocolMessage,
        EthProtocolMessageType.POOLED_TRANSACTIONS: PooledTransactionsEthProtocolMessage,
    }

    _V66_TYPE_MAPPINGS = {
        EthProtocolMessageType.GET_BLOCK_HEADERS: GetBlockHeadersV66EthProtocolMessage,
        EthProtocolMessageType.BLOCK_HEADERS: BlockHeadersV66EthProtocolMessage,
        EthProtocolMessageType.GET_BLOCK_BODIES: GetBlockBodiesV66EthProtocolMessage,
        EthProtocolMessageType.BLOCK_BODIES: BlockBodiesV66EthProtocolMessage,
        EthProtocolMessageType.GET_NODE_DATA: GetNodeDataV66EthProtocolMessage,
        EthProtocolMessageType.NODE_DATA: NodeDataV66EthProtocolMessage,
        EthProtocolMessageType.GET_RECEIPTS: GetReceiptsV66EthProtocolMessage,
        EthProtocolMessageType.RECEIPTS: ReceiptsV66EthProtocolMessage,
        EthProtocolMessageType.GET_POOLED_TRANSACTIONS: GetPooledTransactionsV66EthProtocolMessage,
        EthProtocolMessageType.POOLED_TRANSACTIONS: PooledTransactionsV66EthProtocolMessage,
    }

    if eth_common_constants.ETH_PROTOCOL_VERSION == 63:
        _MESSAGE_TYPE_MAPPING.update({EthProtocolMessageType.STATUS: StatusEthProtocolMessageV63})
    else:
        _MESSAGE_TYPE_MAPPING.update({EthProtocolMessageType.STATUS: StatusEthProtocolMessage})

    def __init__(self, rlpx_cipher: RLPxCipher):
        super(EthProtocolMessageFactory, self).__init__()

        self.message_type_mapping = self._MESSAGE_TYPE_MAPPING
        self._framed_input_buffer = FramedInputBuffer(rlpx_cipher)
        self._expected_msg_type = None

    def get_base_message_type(self) -> Type[AbstractMessage]:
        return EthProtocolMessage

    def set_expected_msg_type(self, msg_type):
        if msg_type not in [EthProtocolMessageType.AUTH, EthProtocolMessageType.AUTH_ACK]:
            raise ValueError("msg_type can be AUTH or AUTH_ACK")

        self._expected_msg_type = msg_type

    def reset_expected_msg_type(self):
        self._expected_msg_type = None

    def set_mappings_for_version(self, version: int) -> None:
        if version >= 66:
            self.message_type_mapping.update(self._V66_TYPE_MAPPINGS)

    def get_message_header_preview_from_input_buffer(self, input_buffer: InputBuffer) -> MessagePreview:
        """
        Peeks at a message, determining if its full.
        Returns (is_full_message, command, payload_length)
        """
        if self._expected_msg_type == EthProtocolMessageType.AUTH:
            return MessagePreview(True, EthProtocolMessageType.AUTH, input_buffer.length)
        elif self._expected_msg_type == EthProtocolMessageType.AUTH_ACK:
            auth_ack_len = self._framed_input_buffer.peek_eip8_handshake_message_len(input_buffer)
            if auth_ack_len:
                return MessagePreview(True, EthProtocolMessageType.AUTH_ACK, auth_ack_len)
        elif self._expected_msg_type is None:
            decryption_start_time = time.time()
            is_full_msg, command = self._framed_input_buffer.peek_message(input_buffer)
            eth_gateway_stats_service.log_decrypted_message(time.time() - decryption_start_time)

            if is_full_msg:
                return MessagePreview(True, command, 0)

        return MessagePreview(False, None, None)

    def create_message_from_buffer(self, buf):
        """
        Parses a full message from a buffer based on its command into one of the loaded message types.
        """

        if self._expected_msg_type is not None:
            return self.base_message_type.initialize_class(RawEthProtocolMessage, buf, None)

        if len(buf) != 0:
            raise ParseError("All bytes are expected to be already read by self._framed_input_buffer")

        message_bytes, command = self._framed_input_buffer.get_full_message()

        return self.create_message(command, message_bytes)
