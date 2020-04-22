import struct
from bxcommon import constants
from bxcommon.messages.abstract_internal_message import AbstractInternalMessage
from bxcommon.messages.versioning.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.gateway.v1.gateway_hello_message_v1 import GatewayHelloMessageV1
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage as GatewayHelloMessageV2
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage


class GatewayHelloMessageConverterV1(AbstractMessageConverter):
    def convert_to_older_version(self, msg: AbstractInternalMessage) -> AbstractInternalMessage:

        if not isinstance(msg, GatewayHelloMessageV2):
            raise TypeError("GatewayHelloMessage is expected")

        msg_bytes = msg.rawbytes()
        mem_view = memoryview(msg_bytes)

        result_bytes = bytearray(len(msg_bytes) - GatewayHelloMessageV2.ADDITIONAL_LENGTH)
        payload_len = GatewayHelloMessageV1.PAYLOAD_LENGTH

        result_bytes[:] = mem_view[:len(msg_bytes) - GatewayHelloMessageV2.ADDITIONAL_LENGTH]

        return AbstractBloxrouteMessage.initialize_class(GatewayHelloMessageV1, result_bytes, (GatewayMessageType.HELLO, payload_len))

    def convert_from_older_version(self, msg: AbstractInternalMessage) -> AbstractInternalMessage:

        if not isinstance(msg, GatewayHelloMessageV1):
            raise TypeError("GatewayHelloMessageV1 is expected")

        msg_bytes = msg.rawbytes()
        mem_view = memoryview(msg_bytes)

        result_bytes = bytearray(len(msg_bytes) + constants.NODE_ID_SIZE_IN_BYTES)
        payload_len = GatewayHelloMessageV2.PAYLOAD_LENGTH
        off = 0
        result_bytes[off:len(msg_bytes)] = mem_view[:]
        off = len(msg_bytes)
        struct.pack_into("%ss" % constants.NODE_ID_SIZE_IN_BYTES, result_bytes, off, b'')

        return AbstractBloxrouteMessage.initialize_class(GatewayHelloMessageV2, result_bytes, (GatewayMessageType.HELLO, payload_len))


    def convert_first_bytes_to_older_version(self, first_msg_bytes):
        raise NotImplementedError()

    def convert_first_bytes_from_older_version(self, first_msg_bytes):
        raise NotImplementedError()

    def get_message_size_change_to_older_version(self):
        raise NotImplementedError()

    def get_message_size_change_from_older_version(self):
        raise NotImplementedError()


# pyre-fixme[45]: Cannot instantiate abstract class `GatewayHelloMessageConverterV1`.
gateway_hello_message_converter_v1 = GatewayHelloMessageConverterV1()
