from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxgateway.messages.gateway.v1.gateway_hello_message_converter_v1 import gateway_hello_message_converter_v1
from bxcommon.messages.versioning.abstract_version_converter_factory import AbstractMessageConverterFactory


class _GatewayMessageConverterFactoryV1(AbstractMessageConverterFactory):

    _MESSAGE_CONVERTER_MAPPING = {
        GatewayMessageType.HELLO: gateway_hello_message_converter_v1,
    }

    def get_message_converter(self, msg_type):
        if not msg_type:
            raise ValueError("msg_type is required.")

        if msg_type not in self._MESSAGE_CONVERTER_MAPPING:
            raise ValueError("Converter for message type '{}' is not defined.".format(msg_type))

        return self._MESSAGE_CONVERTER_MAPPING[msg_type]


gateway_message_converter_factory_v1 = _GatewayMessageConverterFactoryV1()
