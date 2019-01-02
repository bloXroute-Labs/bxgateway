import time

from mock import patch, MagicMock

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import LOCALHOST
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import ObjectHash
from bxgateway import gateway_constants
from bxgateway.gateway_constants import NeutralityPolicy
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message_converter import BtcMessageConverter
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.serializers.block import Block
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxgateway.services.neutrality_service import NeutralityService
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


def mock_connection(message_converter=None, connection_type=None):
    if message_converter is None:
        message_converter = MagicMock()
    connection = MagicMock()
    connection.message_converter = message_converter
    connection.CONNECTION_TYPE = connection_type
    return connection


class NeutralityServiceTest(AbstractTestCase):
    BYTE_BLOCK = helpers.generate_bytearray(30)
    BLOCK_HASH = ObjectHash(crypto.double_sha256("123"))
    KEY_HASH = crypto.double_sha256("234")

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

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(0, len(self.node.broadcast_messages))

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
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

        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(0, len(self.node.broadcast_messages))
        self.neutrality_service.record_block_receipt(self.BLOCK_HASH)
        self.assertEqual(1, len(self.node.broadcast_messages))

        self._assert_broadcast_key()

    def test_propagate_block_to_gateways_and_key_after_timeout(self):
        self.node.in_progress_blocks.get_encryption_key = MagicMock(return_value=self.KEY_HASH)
        self.neutrality_service.register_for_block_receipts(self.BLOCK_HASH, self.BYTE_BLOCK)
        time.time = MagicMock(return_value=time.time() + gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S)

        self.node.alarm_queue.fire_alarms()

        self.assertEqual(2, len(self.node.broadcast_messages))
        self._assert_broadcast_key()
        block_request_messages = filter(lambda broadcasted:
                                        broadcasted[0].msg_type() == GatewayMessageType.BLOCK_PROPAGATION_REQUEST,
                                        self.node.broadcast_messages)
        block_request_message, connection_type = block_request_messages[0]
        self.assertEqual(ConnectionType.GATEWAY, connection_type)
        self.assertEqual(GatewayMessageType.BLOCK_PROPAGATION_REQUEST, block_request_message.msg_type())
        self.assertEqual(self.BYTE_BLOCK, block_request_message.blob())

    def test_propagate_btc_block_to_network(self):
        btc_hash = BtcObjectHash(binary=self.BLOCK_HASH.binary)
        block_message = BlockBtcMessage(12345, 12345, btc_hash, btc_hash, 1, 2, 3, [])
        self._test_propagate_block_to_network(block_message, mock_connection(BtcMessageConverter(12345)))

    def test_propagate_eth_block_to_network(self):
        block = Block(mock_eth_messages.get_dummy_block_header(1), [], [])
        block_message = NewBlockEthProtocolMessage(None, block, 10)
        self._test_propagate_block_to_network(block_message, mock_connection(EthMessageConverter()))

    def _test_propagate_block_to_network(self, block_message, connection):
        self.neutrality_service.propagate_block_to_network(block_message, connection)

        self.assertEqual(1, len(self.node.broadcast_messages))
        broadcast_message, connection_type = self.node.broadcast_messages[0]
        self.assertEqual(ConnectionType.RELAY, connection_type)

        raw_block_hash = bytes(broadcast_message.msg_hash().binary)
        cache_item = self.node.in_progress_blocks._cache.get(raw_block_hash)
        self.assertEqual(cache_item.payload,
                         crypto.symmetric_decrypt(cache_item.key, broadcast_message.blob().tobytes()))
        self.assertIn(broadcast_message.msg_hash(), self.neutrality_service._receipt_tracker)

    def _add_num_gateway_connections(self, count):
        for i in xrange(count):
            self.node.connection_pool.add(i, LOCALHOST, 8000 + i,
                                          mock_connection(connection_type=ConnectionType.GATEWAY))

    def _assert_broadcast_key(self):
        key_messages = filter(lambda broadcasted: broadcasted[0].msg_type() == BloxrouteMessageType.KEY,
                              self.node.broadcast_messages)
        key_message, connection_type = key_messages[0]
        self.assertEqual(ConnectionType.RELAY, connection_type)
        self.assertEqual(BloxrouteMessageType.KEY, key_message.msg_type())
        self.assertEqual(self.KEY_HASH, key_message.key())