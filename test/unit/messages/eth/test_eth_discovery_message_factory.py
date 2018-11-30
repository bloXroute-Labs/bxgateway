import time

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.buffers.input_buffer import InputBuffer
from bxgateway import eth_constants
from bxgateway.eth_exceptions import WrongMACError
from bxgateway.messages.eth.discovery.eth_discovery_message_factory import eth_discovery_message_factory
from bxgateway.messages.eth.discovery.ping_eth_discovery_message import PingEthDiscoveryMessage
from bxgateway.utils.eth import crypto_utils


class EthDiscoveryMessageFactoryTests(AbstractTestCase):
    test_msg_type = PingEthDiscoveryMessage.msg_type
    parser = eth_discovery_message_factory

    def setUp(self):
        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))

        self.test_ping_msg = PingEthDiscoveryMessage(None, dummy_private_key,
                                                     1,
                                                     (u"127.0.0.1", 30001, 30002),
                                                     (u"127.0.0.1", 30003, 30004),
                                                     int(time.time()))

    def test_peek_message__full_message(self):
        msg_bytes, expected_msg_len = self._create_full_msg_bytes()
        is_full, msg_type, msg_len = self.parser.get_message_header_preview(self._to_input_buffer(msg_bytes))
        self.assertTrue(is_full)
        self.assertEqual(msg_type, self.test_msg_type)
        self.assertEqual(msg_len, expected_msg_len)

    def test_peek_message__incomplete_message(self):
        msg_bytes, expected_msg_len = self._create_half_payload_msg_bytes()
        is_full, msg_type, msg_len = self.parser.get_message_header_preview(self._to_input_buffer(msg_bytes))
        self.assertFalse(is_full)
        self.assertEqual(msg_type, self.test_msg_type)
        self.assertEqual(msg_len, expected_msg_len)

    def test_peek_message__incomplete_message_2(self):
        msg_bytes, expected_msg_len = self._create_half_msg_bytes()
        is_full, msg_type, msg_len = self.parser.get_message_header_preview(self._to_input_buffer(msg_bytes))
        self.assertFalse(is_full)
        self.assertIsNone(msg_type)
        self.assertIsNone(msg_len)

    def test_peek_message__longer_message(self):
        msg_bytes, expected_msg_len = self._create_longer_msg_bytes()
        is_full, msg_type, msg_len = self.parser.get_message_header_preview(self._to_input_buffer(msg_bytes))
        self.assertTrue(is_full)
        self.assertEqual(msg_type, self.test_msg_type)
        self.assertEqual(msg_len, expected_msg_len)

    def test_parse__full_message(self):
        msg_bytes, _ = self._create_full_msg_bytes()
        msg = self.parser.create_message_from_buffer(msg_bytes)
        msg.deserialize()
        self.assertTrue(msg)
        self.assertIsInstance(msg, PingEthDiscoveryMessage)
        self.assertEqual(msg.get_protocol_version(), self.test_ping_msg.get_protocol_version())
        self.assertEqual(msg.get_listen_address()[0], self.test_ping_msg.get_listen_address()[0])
        self.assertEqual(msg.get_listen_address()[1], self.test_ping_msg.get_listen_address()[1])
        self.assertEqual(msg.get_listen_address()[2], self.test_ping_msg.get_listen_address()[2])
        self.assertEqual(msg.get_remote_address()[0], self.test_ping_msg.get_remote_address()[0])
        self.assertEqual(msg.get_remote_address()[1], self.test_ping_msg.get_remote_address()[1])
        self.assertEqual(msg.get_remote_address()[2], self.test_ping_msg.get_remote_address()[2])
        self.assertEqual(msg.get_expiration(), self.test_ping_msg.get_expiration())

    def test_parse__incomplete_message(self):
        msg_bytes, _ = self._create_half_payload_msg_bytes()
        msg = self.parser.create_message_from_buffer(msg_bytes)
        self.assertRaises(WrongMACError, msg.deserialize)

    def test_parse__longer_message(self):
        msg_bytes, _ = self._create_longer_msg_bytes()
        msg = self.parser.create_message_from_buffer(msg_bytes)
        self.assertRaises(WrongMACError, msg.deserialize)

    def _create_full_msg_bytes(self):
        msg_bytes = bytearray(self.test_ping_msg.rawbytes())
        msg_len = len(msg_bytes)
        return msg_bytes, msg_len

    def _create_half_payload_msg_bytes(self):
        msg_bytes, msg_len = self._create_full_msg_bytes()
        # removing few bytes
        bytes_to_remove = (len(msg_bytes) - eth_constants.MDC_LEN - eth_constants.SIGNATURE_LEN) / 2
        incomplete_msg_bytes = msg_bytes[:len(msg_bytes) - bytes_to_remove]

        return incomplete_msg_bytes, msg_len

    def _create_half_msg_bytes(self):
        msg_bytes, msg_len = self._create_full_msg_bytes()
        # removing few bytes
        bytes_to_remove = len(msg_bytes) / 2
        incomplete_msg_bytes = msg_bytes[:len(msg_bytes) - bytes_to_remove]

        return incomplete_msg_bytes, msg_len

    def _create_longer_msg_bytes(self):
        msg_bytes, msg_len = self._create_full_msg_bytes()
        # adding some random bytes in the end
        msg_bytes += helpers.generate_bytearray(123)
        return msg_bytes, msg_len

    def _to_input_buffer(self, msg_bytes):
        input_buffer = InputBuffer()
        input_buffer.add_bytes(msg_bytes)
        return input_buffer
