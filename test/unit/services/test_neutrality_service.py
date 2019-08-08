import datetime
import time

from mock import patch, MagicMock

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import LOCALHOST
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants
from bxgateway.gateway_constants import NeutralityPolicy
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.block_info import BlockInfo


def mock_connection(message_converter=None, connection_type=None):
    if message_converter is None:
        message_converter = MagicMock()
    connection = MagicMock()
    connection.message_converter = message_converter
    connection.CONNECTION_TYPE = connection_type
    connection.peer_desc = "127.0.0.1 8000"
    return connection


class NeutralityServiceTest(AbstractTestCase):
    BYTE_BLOCK = helpers.generate_bytearray(29) + b'\x01'
    BLOCK_HASH = Sha256Hash(crypto.double_sha256(b"123"))
    KEY_HASH = crypto.double_sha256(b"234")
    MOCK_CONNECTION = mock_connection(connection_type=ConnectionType.GATEWAY)

    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000,
                                                             include_default_btc_args=True,
                                                             include_default_eth_args=True))
        self.neutrality_service = NeutralityService(self.node)
        self.node.neutrality_service = self.neutrality_service

    @patch("bxgateway.gateway_constants.NEUTRALITY_POLICY", NeutralityPolicy.RELEASE_IMMEDIATELY)
    def test_release_key_immediately(self):
        self.node.in_progress_blocks.get_encryption_key = MagicMock(return_value=self.KEY_HASH)
        self.neutrality_service.register_for_block_receipts(self.BLOCK_HASH, self.BYTE_BLOCK)
        self._assert_broadcast_key()

    @patch("bxgateway.gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_COUNT", 2)
    @patch("bxgateway.gateway_constants.NEUTRALITY_POLICY", NeutralityPolicy.RECEIPT_COUNT)
    def test_release_key_after_enough_receipts_count(self):
        self.node.in_progress_blocks.get_encryption_key = MagicMock(return_value=self.KEY_HASH)

        self.neutrality_service.register_for_block_receipts(self.BLOCK_HASH, self.BYTE_BLOCK)

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH, self.MOCK_CONNECTION)
        self.assertEqual(0, len(self.node.broadcast_messages))

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH, self.MOCK_CONNECTION)
        self.assertEqual(1, len(self.node.broadcast_messages))

        self._assert_broadcast_key()

    @patch("bxgateway.gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_PERCENT", 50)
    @patch("bxgateway.gateway_constants.NEUTRALITY_POLICY", NeutralityPolicy.RECEIPT_PERCENT)
    def test_release_key_after_enough_receipts_percent(self):
        self._add_num_gateway_connections(6)
        self.node.in_progress_blocks.get_encryption_key = MagicMock(return_value=self.KEY_HASH)

        self.neutrality_service.register_for_block_receipts(self.BLOCK_HASH, self.BYTE_BLOCK)

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(1, len(self.node.broadcast_messages))

        self._assert_broadcast_key()

    @patch("bxgateway.gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_COUNT", 4)
    @patch("bxgateway.gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_PERCENT", 50)
    @patch("bxgateway.gateway_constants.NEUTRALITY_POLICY", NeutralityPolicy.RECEIPT_COUNT_AND_PERCENT)
    def test_release_key_after_enough_receipts_percent(self):
        self._add_num_gateway_connections(6)
        self.node.in_progress_blocks.get_encryption_key = MagicMock(return_value=self.KEY_HASH)

        self.neutrality_service.register_for_block_receipts(self.BLOCK_HASH, self.BYTE_BLOCK)

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH, self.MOCK_CONNECTION)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH, self.MOCK_CONNECTION)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH, self.MOCK_CONNECTION)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH, self.MOCK_CONNECTION)
        self.assertEqual(1, len(self.node.broadcast_messages))

        self._assert_broadcast_key()

    def test_propagate_block_to_gateways_and_key_after_timeout(self):
        self.node.in_progress_blocks.get_encryption_key = MagicMock(return_value=self.KEY_HASH)
        self.neutrality_service.register_for_block_receipts(self.BLOCK_HASH, self.BYTE_BLOCK)
        time.time = MagicMock(return_value=time.time() + gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S)

        self.node.alarm_queue.fire_alarms()

        self.assertEqual(2, len(self.node.broadcast_messages))
        self._assert_broadcast_key()
        block_request_messages = list(filter(lambda broadcasted:
                                             broadcasted[0].msg_type() == GatewayMessageType.BLOCK_PROPAGATION_REQUEST,
                                             self.node.broadcast_messages))
        block_request_message, connection_types = block_request_messages[0]
        self.assertEqual(ConnectionType.GATEWAY, connection_types[0])
        self.assertEqual(GatewayMessageType.BLOCK_PROPAGATION_REQUEST, block_request_message.msg_type())
        self.assertEqual(self.BYTE_BLOCK, block_request_message.blob())

    def test_propagate_block_to_network_encrypted_block(self):
        self.node.opts.encrypt_blocks = True

        block_message = helpers.generate_bytearray(50)
        connection = MockConnection(MockSocketConnection(1), (LOCALHOST, 9000), self.node)
        self.neutrality_service.propagate_block_to_network(block_message, connection)

        self.assertEqual(1, len(self.node.broadcast_messages))
        broadcast_message, connection_types = self.node.broadcast_messages[0]
        self.assertTrue(ConnectionType.RELAY_BLOCK & connection_types[0])

        raw_block_hash = bytes(broadcast_message.block_hash().binary)
        cache_item = self.node.in_progress_blocks._cache.get(raw_block_hash)
        self.assertEqual(cache_item.payload,
                         crypto.symmetric_decrypt(cache_item.key, broadcast_message.blob().tobytes()))
        self.assertIn(broadcast_message.block_hash(), self.neutrality_service._receipt_tracker)

    def test_propagate_block_to_network_unencrypted_block(self):
        self.node.opts.encrypt_blocks = False

        block_message = helpers.generate_bytearray(50)
        block_info = BlockInfo(Sha256Hash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN)), [],
                               datetime.datetime.utcnow(), datetime.datetime.utcnow(), 0, 1,
                               helpers.generate_bytearray(crypto.SHA256_HASH_LEN),
                               helpers.generate_bytearray(crypto.SHA256_HASH_LEN),
                               0, 0, 0)

        connection = MockConnection(MockSocketConnection(1), (LOCALHOST, 9000), self.node)
        self.neutrality_service.propagate_block_to_network(block_message, connection, block_info)

        self.assertEqual(1, len(self.node.broadcast_messages))
        broadcast_message, connection_types = self.node.broadcast_messages[0]
        # self.assertTrue(any(ConnectionType.RELAY_BLOCK & connection_type for connection_type in connection_types))
        self.assertTrue(all(ConnectionType.RELAY_BLOCK & connection_type for connection_type in connection_types))
        self.assertEqual(block_info.block_hash, broadcast_message.block_hash())

        self.assertNotIn(block_info.block_hash, self.node.in_progress_blocks._cache)
        self.assertNotIn(broadcast_message.block_hash(), self.neutrality_service._receipt_tracker)

    def _add_num_gateway_connections(self, count):
        for i in range(count):
            self.node.connection_pool.add(i, LOCALHOST, 8000 + i,
                                          mock_connection(connection_type=ConnectionType.GATEWAY))

    def _assert_broadcast_key(self):
        key_messages = list(filter(lambda broadcasted: broadcasted[0].msg_type() == BloxrouteMessageType.KEY,
                                   self.node.broadcast_messages))
        key_message, connection_types = key_messages[0]
        self.assertTrue(ConnectionType.RELAY_ALL & connection_types[0])
        self.assertEqual(BloxrouteMessageType.KEY, key_message.msg_type())
        self.assertEqual(self.KEY_HASH, key_message.key())
