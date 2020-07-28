from typing import Type

from bxcommon.exceptions import ParseError, UnrecognizedCommandError
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory, MessagePreview
from bxcommon.utils.buffers.input_buffer import InputBuffer
from bxgateway.messages.eth.discovery.eth_discovery_message import EthDiscoveryMessage
from bxgateway.messages.eth.discovery.eth_discovery_message_type import EthDiscoveryMessageType
from bxgateway.messages.eth.discovery.ping_eth_discovery_message import PingEthDiscoveryMessage
from bxgateway.messages.eth.discovery.pong_eth_discovery_message import PongEthDiscoveryMessage
from bxcommon.utils.blockchain_utils.eth import rlp_utils, eth_common_constants


class _EthDiscoveryMessageFactory(AbstractMessageFactory):
    _MESSAGE_TYPE_MAPPING = {
        EthDiscoveryMessageType.PING: PingEthDiscoveryMessage,
        EthDiscoveryMessageType.PONG: PongEthDiscoveryMessage
    }

    def __init__(self):
        super(_EthDiscoveryMessageFactory, self).__init__()
        self.message_type_mapping = self._MESSAGE_TYPE_MAPPING

    def get_base_message_type(self) -> Type[AbstractMessage]:
        return EthDiscoveryMessage

    def get_message_header_preview_from_input_buffer(self, input_buffer: InputBuffer) -> MessagePreview:
        """
        Peeks at a message, determining if its full.
        Returns (is_full_message, command, payload_length)
        """
        # pyre-fixme[25]: Assertion will always fail.
        if not isinstance(input_buffer, InputBuffer):
            raise ValueError("InputBuffer type is expected")

        msg_type = self._peek_message_type(input_buffer)

        if msg_type is None:
            return MessagePreview(False, None, None)

        msg_len = self._peek_message_len(input_buffer)

        is_full = input_buffer.length >= msg_len

        return MessagePreview(is_full, msg_type, msg_len)

    def create_message_from_buffer(self, buf):
        """
        Parses a full message from a buffer based on its command into one of the loaded message types.
        """
        command = self._get_message_type(buf)

        if command not in self.message_type_mapping:
            raise UnrecognizedCommandError("Message not recognized: {0}. Raw data: {1}".format(command, repr(buf)), buf)

        message_cls = self.message_type_mapping[command]
        return self.base_message_type.initialize_class(message_cls, buf, None)

    def _peek_message_type(self, input_buffer):
        if not input_buffer:
            return None

        if not isinstance(input_buffer, InputBuffer):
            raise ValueError("InputBuffer type is expected")

        msg_type_position = eth_common_constants.MDC_LEN + eth_common_constants.SIGNATURE_LEN

        if input_buffer.length < msg_type_position + eth_common_constants.MSG_TYPE_LEN:
            return None

        msg_type_bytes = input_buffer.get_slice(msg_type_position, msg_type_position + eth_common_constants.MSG_TYPE_LEN)
        msg_type = rlp_utils.safe_ord(msg_type_bytes)

        return msg_type

    def _peek_message_len(self, input_buffer):
        if not input_buffer:
            return None

        if not isinstance(input_buffer, InputBuffer):
            raise ValueError("InputBuffer type is expected")

        msg_content_position = eth_common_constants.MDC_LEN + eth_common_constants.SIGNATURE_LEN + eth_common_constants.MSG_TYPE_LEN

        if input_buffer.length < msg_content_position:
            return None

        msg_content = input_buffer.get_slice(msg_content_position, input_buffer.length)
        msg_content_memview = memoryview(msg_content)

        _, content_length, length_prefix_size = rlp_utils.consume_length_prefix(msg_content_memview, 0)

        return msg_content_position + length_prefix_size + content_length

    def _get_message_type(self, msg_bytes):
        msg_type_position = eth_common_constants.MDC_LEN + eth_common_constants.SIGNATURE_LEN

        if len(msg_bytes) <= msg_type_position:
            raise ParseError("Message length {0} is less then position of message type.".format(len(msg_bytes)))

        msg_type = rlp_utils.safe_ord(msg_bytes[msg_type_position:msg_type_position + eth_common_constants.MSG_TYPE_LEN])
        return msg_type


eth_discovery_message_factory = _EthDiscoveryMessageFactory()
