from bxgateway.messages.btc.data_btc_message import GetBlocksBtcMessage
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import create_input_buffer_with_message
from bxcommon.utils import crypto
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_factory import gateway_message_factory
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class GatewayMessageFactoryTest(AbstractTestCase):
    def get_message_preview_successfully(self, message, expected_command, expected_payload_length):
        is_full_message, command, payload_length = gateway_message_factory.get_message_header_preview(
            create_input_buffer_with_message(message)
        )
        self.assertTrue(is_full_message)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_payload_length, payload_length)

    def create_message_successfully(self, message, message_type):
        result = gateway_message_factory.create_message_from_buffer(message.rawbytes())
        self.assertIsInstance(result, message_type)
        return result

    def test_message_preview_success_all_gateway_types(self):
        self.get_message_preview_successfully(GatewayHelloMessage(123, 1, "127.0.0.1", 40000, 1),
                                              GatewayHelloMessage.MESSAGE_TYPE, GatewayHelloMessage.PAYLOAD_LENGTH)

    def test_create_message_success_all_gateway_types(self):
        hello_message = self.create_message_successfully(GatewayHelloMessage(123, 1, "127.0.0.1", 40001, 1),
                                                         GatewayHelloMessage)
        self.assertEqual(123, hello_message.protocol_version())
        self.assertEqual(1, hello_message.network_num())
        self.assertEqual("127.0.0.1", hello_message.ip())
        self.assertEqual(40001, hello_message.port())
        self.assertEqual(1, hello_message.ordering())

