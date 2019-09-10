from bxcommon.test_utils.abstract_test_case import AbstractTestCase

from bxgateway.messages.gateway.v1.gateway_hello_message_v1 import GatewayHelloMessageV1
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage as GatewayHelloMessageV2
from bxgateway.messages.gateway.gateway_version_manager import gateway_version_manager


class GatewayVersionManagerTest(AbstractTestCase):

    def test_convert_message_to_older_version__hello_message_v1(self):

        hello_msg = GatewayHelloMessageV2(protocol_version=gateway_version_manager.CURRENT_PROTOCOL_VERSION,
                                          network_num=1, ip="127.0.0.1", port=40000, ordering=1,
                                          node_id="0b848e61-a269-4cbe-b1ff-f276066d3f8b")

        hello_msg_v1 = gateway_version_manager.convert_message_to_older_version(1, hello_msg)
        self.assertIsInstance(hello_msg_v1, GatewayHelloMessageV1)
        self.assertEqual(2, hello_msg.protocol_version())
        self.assertEqual(hello_msg_v1.network_num(), hello_msg.network_num())
        self.assertEqual(hello_msg_v1.ip(), hello_msg.ip())
        self.assertEqual(hello_msg_v1.port(), hello_msg.port())
        self.assertEqual(hello_msg_v1.ordering(), hello_msg.ordering())
        self.assertEqual("0b848e61-a269-4cbe-b1ff-f276066d3f8b", hello_msg.node_id())

    def test_convert_message_from_older_version__hello_message_v1(self):
        hello_msg_v1 = GatewayHelloMessageV1(protocol_version=1, network_num=1, ip="127.0.0.1", port=40000, ordering=1)
        hello_msg = gateway_version_manager.convert_message_from_older_version(1, hello_msg_v1)

        self.assertIsInstance(hello_msg, GatewayHelloMessageV2)
        self.assertEqual(hello_msg_v1.network_num(), hello_msg.network_num())
        self.assertEqual(hello_msg_v1.ip(), hello_msg.ip())
        self.assertEqual(hello_msg_v1.port(), hello_msg.port())
        self.assertEqual(hello_msg_v1.ordering(), hello_msg.ordering())

