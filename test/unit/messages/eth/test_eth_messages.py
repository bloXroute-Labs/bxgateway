import collections
import time

import rlp

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils import helpers
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import eth_constants
from bxgateway.messages.eth.discovery.ping_eth_discovery_message import PingEthDiscoveryMessage
from bxgateway.messages.eth.discovery.pong_eth_discovery_message import PongEthDiscoveryMessage
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.disconnect_eth_protocol_message import DisconnectEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import GetBlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_node_data_eth_protocol_message import GetNodeDataEthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_eth_protocol_message import GetReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.hello_eth_protocol_message import HelloEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import NewBlockHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.node_data_eth_protocol_message import NodeDataEthProtocolMessage
from bxgateway.messages.eth.protocol.ping_eth_protocol_message import PingEthProtocolMessage
from bxgateway.messages.eth.protocol.receipts_eth_protocol_message import ReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.messages.eth.serializers.block import Block
from bxgateway.messages.eth.serializers.block_hash import BlockHash
from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.utils.eth import crypto_utils


class EthMessagesTests(AbstractTestCase):

    def test_discovery_ping_eth_message(self):
        self._test_msg_serialization(PingEthDiscoveryMessage,
                                     True,
                                     eth_constants.P2P_PROTOCOL_VERSION,
                                     # random addresses
                                     ("192.1.2.3", 11111, 22222),
                                     ("193.4.5.6", 33333, 44444),
                                     int(time.time()))

    def test_discovery_pong_eth_message(self):
        self._test_msg_serialization(PongEthDiscoveryMessage,
                                     True,
                                     ("192.1.2.3", 11111, 22222),
                                     helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                     int(time.time()))

    def test_hello_eth_message(self):
        dummy_private_key = convert.hex_to_bytes("294549f8629f0eeb2b8e01aca491f701f5386a9662403b485c4efe7d447dfba3")
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)

        self._test_msg_serialization(HelloEthProtocolMessage,
                                     False,
                                     eth_constants.P2P_PROTOCOL_VERSION,
                                     eth_constants.BX_ETH_CLIENT_NAME,
                                     eth_constants.CAPABILITIES,
                                     30303,  # random port value
                                     dummy_public_key)

    def test_ping_eth_message(self):
        self._test_msg_serialization(PingEthProtocolMessage, False)

    def test_pong_eth_message(self):
        self._test_msg_serialization(PingEthProtocolMessage, False)

    def test_status_eth_message(self):
        dummy_network_id = 111
        dummy_chain_difficulty = 11111
        dummy_chain_head_hash = convert.hex_to_bytes("f973c5d3763c40e2b5080f35a9003e64e7d9f9d429ddecd7c559cbd4061094cd")
        dummy_genesis_hash = convert.hex_to_bytes("aec175735fb6b74722d54455b638f6340bb9fdd5fa8101c8c0869d10cdccb000")

        self._test_msg_serialization(StatusEthProtocolMessage,
                                     False,
                                     eth_constants.ETH_PROTOCOL_VERSION,
                                     dummy_network_id,
                                     dummy_chain_difficulty,
                                     dummy_chain_head_hash,
                                     dummy_genesis_hash)

    def test_disconnect_message(self):
        dummy_disconnect_reason = 3
        self._test_msg_serialization(DisconnectEthProtocolMessage, False, [dummy_disconnect_reason])

    def test_new_block_hashes_eth_message(self):
        self._test_msg_serialization(NewBlockHashesEthProtocolMessage,
                                     False,
                                     # passing few dummy block hashes
                                     [
                                         BlockHash(helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN), 111),
                                         BlockHash(helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN), 222),
                                         BlockHash(helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN), 333)
                                     ])

    def test_get_block_headers_eth_message(self):
        self._test_msg_serialization(GetBlockHeadersEthProtocolMessage,
                                     False,
                                     helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                     111,
                                     222,
                                     0)

    def test_block_header_eth_message(self):
        self._test_msg_serialization(BlockHeadersEthProtocolMessage,
                                     False,
                                     [
                                         mock_eth_messages.get_dummy_block_header(1),
                                         mock_eth_messages.get_dummy_block_header(2),
                                         mock_eth_messages.get_dummy_block_header(3)
                                     ])

    def test_get_block_bodies_eth_message(self):
        self._test_msg_serialization(GetBlockBodiesEthProtocolMessage,
                                     False,
                                     # passing randomly generated hashes
                                     [
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN)
                                     ])

    def test_block_bodies_eth_message(self):
        self._test_msg_serialization(BlockBodiesEthProtocolMessage,
                                     False,
                                     # passing randomly generated hashes
                                     [
                                         mock_eth_messages.get_dummy_transient_block_body(1),
                                         mock_eth_messages.get_dummy_transient_block_body(2),
                                         mock_eth_messages.get_dummy_transient_block_body(3)
                                     ])

    def test_transactions_eth_message(self):
        self._test_msg_serialization(TransactionsEthProtocolMessage,
                                     False,
                                     [
                                         mock_eth_messages.get_dummy_transaction(1),
                                         mock_eth_messages.get_dummy_transaction(2),
                                         mock_eth_messages.get_dummy_transaction(3)
                                     ])

    def test_new_block_eth_message(self):
        self._test_msg_serialization(NewBlockEthProtocolMessage,
                                     False,
                                     mock_eth_messages.get_dummy_block(1),
                                     111)

    def test_get_node_data_eth_message(self):
        self._test_msg_serialization(GetNodeDataEthProtocolMessage,
                                     False,
                                     # passing randomly generated hashes
                                     [
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN)
                                     ])

    def test_node_data_eth_message(self):
        self._test_msg_serialization(NodeDataEthProtocolMessage,
                                     False,
                                     helpers.generate_bytes(1000))

    def test_get_receipts_eth_message(self):
        self._test_msg_serialization(GetReceiptsEthProtocolMessage,
                                     False,
                                     # passing randomly generated hashes
                                     [
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
                                         helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN)
                                     ])

    def test_receipts_eth_message(self):
        self._test_msg_serialization(ReceiptsEthProtocolMessage,
                                     False,
                                     helpers.generate_bytes(1000))

    def test_new_block_internal_eth_message_to_from_new_block_message(self):
        txs = []
        txs_bytes = []
        txs_hashes = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = mock_eth_messages.get_dummy_transaction(1)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

        block_header = mock_eth_messages.get_dummy_block_header(1)

        uncles = [
            mock_eth_messages.get_dummy_block_header(2),
            mock_eth_messages.get_dummy_block_header(3),
        ]

        block = Block(
            block_header,
            txs,
            uncles
        )

        dummy_chain_difficulty = 10

        block_msg = NewBlockEthProtocolMessage(None, block, dummy_chain_difficulty)
        self.assertTrue(block_msg.rawbytes())

        new_block_internal_eth_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        self.assertIsNotNone(new_block_internal_eth_msg)
        self.assertTrue(new_block_internal_eth_msg.rawbytes())

        parsed_new_block_message = new_block_internal_eth_msg.to_new_block_msg()
        self.assertIsInstance(parsed_new_block_message, NewBlockEthProtocolMessage)

        self.assertEqual(block_msg.rawbytes(), parsed_new_block_message.rawbytes())

    def test_new_block_internal_eth_message_to_from_new_block_message(self):
        txs = []
        txs_bytes = []
        txs_hashes = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = mock_eth_messages.get_dummy_transaction(1)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

        block_header = mock_eth_messages.get_dummy_block_header(1)

        uncles = [
            mock_eth_messages.get_dummy_block_header(2),
            mock_eth_messages.get_dummy_block_header(3),
        ]

        block = Block(
            block_header,
            txs,
            uncles
        )

        dummy_chain_difficulty = 10

        block_msg = NewBlockEthProtocolMessage(None, block, dummy_chain_difficulty)
        self.assertTrue(block_msg.rawbytes())

        new_block_internal_eth_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        self.assertIsNotNone(new_block_internal_eth_msg)
        self.assertTrue(new_block_internal_eth_msg.rawbytes())

        parsed_new_block_message = new_block_internal_eth_msg.to_new_block_msg()
        self.assertIsInstance(parsed_new_block_message, NewBlockEthProtocolMessage)

        self.assertEqual(len(block_msg.rawbytes()), len(parsed_new_block_message.rawbytes()))
        self.assertEqual(convert.bytes_to_hex(block_msg.rawbytes()),
                         convert.bytes_to_hex(parsed_new_block_message.rawbytes()))

    def test_new_block_internal_eth_message_to_from_new_block_parts(self):
        txs = []
        txs_bytes = []
        txs_hashes = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = mock_eth_messages.get_dummy_transaction(1)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

        block_header = mock_eth_messages.get_dummy_block_header(1)

        uncles = [
            mock_eth_messages.get_dummy_block_header(2),
            mock_eth_messages.get_dummy_block_header(3),
        ]

        block_number = 100000

        block_body = TransientBlockBody(txs, uncles)

        block_header_bytes = memoryview(rlp.encode(BlockHeader.serialize(block_header)))
        block_body_bytes = memoryview(rlp.encode(TransientBlockBody.serialize(block_body)))

        new_block_parts = NewBlockParts(block_header_bytes, block_body_bytes, block_number)

        new_block_internal_eth_msg = InternalEthBlockInfo.from_new_block_parts(new_block_parts)
        self.assertIsNotNone(new_block_internal_eth_msg)
        self.assertTrue(new_block_internal_eth_msg.rawbytes())

        parsed_new_block_parts = new_block_internal_eth_msg.to_new_block_parts()
        self.assertIsInstance(parsed_new_block_parts, NewBlockParts)

        self.assertEqual(block_header_bytes, parsed_new_block_parts.block_header_bytes)
        self.assertEqual(block_body_bytes, parsed_new_block_parts.block_body_bytes)
        self.assertEqual(block_number, parsed_new_block_parts.block_number)

    def test_block_headers_msg_from_header_bytes(self):
        block_header = mock_eth_messages.get_dummy_block_header(1)
        block_header_bytes = memoryview(rlp.encode(BlockHeader.serialize(block_header)))

        block_headers_msg = BlockHeadersEthProtocolMessage.from_header_bytes(block_header_bytes)
        raw_headers = block_headers_msg.get_block_headers()
        headers_list = list(raw_headers)
        self.assertEqual(len(headers_list), 1)
        self.assertTrue(headers_list)
        self.assertEqual(1, len(block_headers_msg.get_block_headers()))
        self.assertEqual(1, len(block_headers_msg.get_block_headers_bytes()))
        self.assertEqual(block_header, block_headers_msg.get_block_headers()[0])
        self.assertEqual(block_header_bytes.tobytes(), block_headers_msg.get_block_headers_bytes()[0].tobytes())

    def test_block_bodies_msg_from_bodies_bytes(self):
        txs = []
        txs_bytes = []
        txs_hashes = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = mock_eth_messages.get_dummy_transaction(1)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

        uncles = [
            mock_eth_messages.get_dummy_block_header(2),
            mock_eth_messages.get_dummy_block_header(3),
        ]

        block_body = TransientBlockBody(txs, uncles)
        block_body_bytes = memoryview(rlp.encode(TransientBlockBody.serialize(block_body)))

        block_bodies_msg = BlockBodiesEthProtocolMessage.from_body_bytes(block_body_bytes)

        self.assertEqual(1, len(block_bodies_msg.get_blocks()))
        self.assertEqual(1, len(block_bodies_msg.get_block_bodies_bytes()))
        self.assertEqual(block_body, block_bodies_msg.get_blocks()[0])
        self.assertEqual(block_body_bytes, block_bodies_msg.get_block_bodies_bytes()[0])

    def test_new_block_hashes_msg_from_block_hash(self):
        block_hash_bytes = helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN)
        block_hash = Sha256Hash(block_hash_bytes)
        block_number = 1000

        new_block_hashes_msg_from_hash = NewBlockHashesEthProtocolMessage.from_block_hash_number_pair(block_hash,
                                                                                                      block_number)
        new_block_hashes_msg_from_hash.deserialize()

        self.assertEqual(1, len(new_block_hashes_msg_from_hash.get_block_hash_number_pairs()))
        deserialized_hash, deserialized_block_number = new_block_hashes_msg_from_hash.get_block_hash_number_pairs()[0]
        self.assertEqual(deserialized_hash, block_hash)
        self.assertEqual(block_number, block_number)

    def test_new_block_parts_to_new_block_message(self):
        txs = []
        txs_bytes = []
        txs_hashes = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = mock_eth_messages.get_dummy_transaction(1)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

        block_header = mock_eth_messages.get_dummy_block_header(1)

        uncles = [
            mock_eth_messages.get_dummy_block_header(2),
            mock_eth_messages.get_dummy_block_header(3),
        ]

        block = Block(
            block_header,
            txs,
            uncles
        )

        dummy_chain_difficulty = 10

        new_block_msg = NewBlockEthProtocolMessage(None, block, dummy_chain_difficulty)
        self.assertTrue(new_block_msg.rawbytes())

        block_body = TransientBlockBody(txs, uncles)

        block_bytes = rlp.encode(Block.serialize(block))
        block_header_bytes = memoryview(rlp.encode(BlockHeader.serialize(block_header)))
        block_body_bytes = memoryview(rlp.encode(TransientBlockBody.serialize(block_body)))
        self.assertEqual(len(block_bytes), len(block_header_bytes) + len(block_body_bytes))

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(new_block_msg)
        new_block_parts = internal_new_block_msg.to_new_block_parts()

        new_block_msg_from_block_parts = NewBlockEthProtocolMessage.from_new_block_parts(new_block_parts,
                                                                                         dummy_chain_difficulty)

        self.assertEqual(len(new_block_msg.rawbytes()), len(new_block_msg_from_block_parts.rawbytes()))
        self.assertEqual(new_block_msg.rawbytes(),
                         new_block_msg_from_block_parts.rawbytes())
        self.assertEqual(new_block_msg_from_block_parts.chain_difficulty(), dummy_chain_difficulty)

    def _test_msg_serialization(self, msg_cls, needs_private_key, *args, **kwargs):
        if needs_private_key:
            # random private key
            private_key = convert.hex_to_bytes("294549f8629f0eeb2b8e01aca491f701f5386a9662403b485c4efe7d447dfba3")
            msg = msg_cls(None, private_key, *args, **kwargs)
        else:
            msg = msg_cls(None, *args, **kwargs)

        # verify that fields values are set correctly
        self._verify_field_values(msg_cls, msg, *args)

        # serialize message into bytes
        msg_bytes = msg.rawbytes()

        # deserialize message from bytes
        msg_deserialized = msg_cls(msg_bytes)
        msg_deserialized.deserialize()

        # verify that field values are correct after deserialization
        self._verify_field_values(msg_cls, msg_deserialized, *args)

        self.assertEqual(msg.msg_type, msg_cls.msg_type)
        self.assertEqual(msg.msg_type, msg_deserialized.msg_type)

    def _verify_field_values(self, msg_cls, msg, *args):

        # test that all field attributes set correctly
        for field, field_value in zip(msg_cls.fields, args):
            field_name, serializer = field

            self.assertTrue(hasattr(msg, field_name))
            attr_value = getattr(msg, field_name)

            self._assert_values_equal(attr_value, field_value)

    def _assert_values_equal(self, actual_value, expected_value, ):

        if isinstance(expected_value, collections.Iterable) and \
                not isinstance(expected_value, bytearray) and \
                not isinstance(expected_value, str):

            for actual_item_value, expected_item_value in zip(actual_value, expected_value):
                self._assert_values_equal(actual_item_value, expected_item_value)

        elif isinstance(expected_value, rlp.Serializable):
            for serializer_field in expected_value.fields:
                serializer_field_name, _ = serializer_field

                actual_field_value = getattr(actual_value, serializer_field_name)
                expected_field_value = getattr(expected_value, serializer_field_name)

                self._assert_values_equal(actual_field_value, expected_field_value)
        else:
            self.assertEqual(actual_value, expected_value)
