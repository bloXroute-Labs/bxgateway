import collections
import hashlib
import time

import rlp

from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.services.transaction_service import TransactionService
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.utils.object_hash import ObjectHash
from bxgateway import eth_constants
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.messages.eth.serializers.block import Block
from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.compact_block import CompactBlock
from bxgateway.messages.eth.serializers.short_transaction import ShortTransaction
from bxgateway.messages.eth.serializers.transaction import Transaction


class EthMessageConverterTests(AbstractTestCase):

    def setUp(self):
        self.tx_service = TransactionService(MockNode("127.0.0.1", 123))
        self.message_parser = EthMessageConverter()
        self.test_network_num = 12345

    def test_tx_to_bx_tx__success(self):
        txs = [
            self._get_dummy_tx(1),
            self._get_dummy_tx(2),
            self._get_dummy_tx(3)
        ]

        tx_msg = TransactionsEthProtocolMessage(None, txs)
        self.assertTrue(tx_msg.rawbytes())

        self.validate_tx_to_bx_txs_conversion(tx_msg, txs)

    def test_tx_to_bx_tx__from_bytes_success(self):
        txs = [
            self._get_dummy_tx(1),
            self._get_dummy_tx(2),
            self._get_dummy_tx(3)
        ]

        tx_msg = TransactionsEthProtocolMessage(None, txs)
        tx_msg_bytes = tx_msg.rawbytes()

        self.assertTrue(tx_msg_bytes)
        tx_msg_from_bytes = TransactionsEthProtocolMessage(tx_msg_bytes)

        self.validate_tx_to_bx_txs_conversion(tx_msg_from_bytes, txs)

    def test_tx_to_bx_tx__from_bytes_single_tx_success(self):
        txs = [
            self._get_dummy_tx(1)
        ]

        tx_msg = TransactionsEthProtocolMessage(None, txs)
        tx_msg_bytes = tx_msg.rawbytes()

        self.assertTrue(tx_msg_bytes)
        tx_msg_from_bytes = TransactionsEthProtocolMessage(tx_msg_bytes)

        self.validate_tx_to_bx_txs_conversion(tx_msg_from_bytes, txs)


    def validate_tx_to_bx_txs_conversion(self, tx_msg, txs):
        bx_tx_msgs = self.message_parser.tx_to_bx_txs(tx_msg, self.test_network_num)

        self.assertTrue(bx_tx_msgs)
        self.assertEqual(len(txs), len(bx_tx_msgs))

        for tx, (bx_tx_msg, tx_hash, tx_bytes) in zip(txs, bx_tx_msgs):
            self.assertIsInstance(bx_tx_msg, TxMessage)
            tx_obj = rlp.decode(bx_tx_msg.tx_val().tobytes(), Transaction)
            self.assertEqual(tx, tx_obj)

    def test_bx_tx_to_tx__success(self):
        tx = self._get_dummy_tx(1)

        tx_bytes = rlp.encode(tx, Transaction)
        tx_hash_bytes = hashlib.sha256(tx_bytes).digest()
        tx_hash = ObjectHash(tx_hash_bytes)

        bx_tx_message = TxMessage(tx_hash=tx_hash, network_num=self.test_network_num, tx_val=tx_bytes)

        tx_message = self.message_parser.bx_tx_to_tx(bx_tx_message)

        self.assertIsNotNone(tx_message)
        self.assertIsInstance(tx_message, TransactionsEthProtocolMessage)

        self.assertTrue(tx_message.get_transactions())
        self.assertEqual(1, len(tx_message.get_transactions()))

        tx_obj = tx_message.get_transactions()[0]
        self.assertEqual(tx, tx_obj)

    def test_block_to_bx_block__success(self):
        txs = []
        txs_bytes = []
        txs_hashes = []
        short_ids = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = self._get_dummy_tx(i)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

            if i % 2 == 0:
                self.tx_service.assign_tx_to_sid(tx_hash, i, time.time())
                short_ids.append(i)
            else:
                short_ids.append(0)

        block = Block(
            self._get_dummy_block_header(1),
            txs,
            [
                self._get_dummy_block_header(2),
                self._get_dummy_block_header(3)
            ]
        )

        dummy_chain_difficulty = 10

        block_msg = NewBlockEthProtocolMessage(None, block, dummy_chain_difficulty)
        self.assertTrue(block_msg.rawbytes())

        bx_block_msg = self.message_parser.block_to_bx_block(block_msg, self.tx_service)

        self.assertTrue(bx_block_msg)
        self.assertIsInstance(bx_block_msg, bytearray)

        compact_block = rlp.decode(bx_block_msg, CompactBlock)
        self.assertTrue(compact_block)
        self.assertIsInstance(compact_block, CompactBlock)

        self._assert_values_equal(compact_block.header, block.header)
        self._assert_values_equal(compact_block.uncles, block.uncles)

        self.assertEqual(len(compact_block.transactions), len(block.transactions))

        for tx, short_tx, i in zip(block.transactions, compact_block.transactions, range(1, tx_count)):
            self.assertIsInstance(tx, Transaction)
            self.assertIsInstance(short_tx, ShortTransaction)

            if i % 2 == 0:
                self.assertEqual(0, short_tx.full_transaction)
                self.assertEqual(short_ids[i - 1], short_tx.short_id)
                self.assertEqual(short_tx.transaction_bytes, bytes())
            else:
                self.assertEqual(1, short_tx.full_transaction)
                self.assertEqual(0, short_tx.short_id)
                self.assertEqual(short_tx.transaction_bytes, txs_bytes[i - 1])

        self.assertEqual(compact_block.chain_difficulty, block_msg.chain_difficulty)

    def test_block_to_bx_block__empty_block_success(self):
        block = Block(self._get_dummy_block_header(8), [], [])

        dummy_chain_difficulty = 10

        block_msg = NewBlockEthProtocolMessage(None, block, dummy_chain_difficulty)
        self.assertTrue(block_msg.rawbytes())

        bx_block_msg = self.message_parser.block_to_bx_block(block_msg, self.tx_service)

        self.assertTrue(bx_block_msg)
        self.assertIsInstance(bx_block_msg, bytearray)

        compact_block = rlp.decode(bx_block_msg, CompactBlock)
        self.assertTrue(compact_block)
        self.assertIsInstance(compact_block, CompactBlock)

        self._assert_values_equal(compact_block.header, block.header)
        self.assertEqual(0, len(compact_block.uncles))
        self.assertEqual(0, len(compact_block.transactions))
        self.assertEqual(compact_block.chain_difficulty, block_msg.chain_difficulty)

    def test_bx_block_to_block__success(self):
        tx_count = 100
        txs = []
        short_txs = []

        for i in range(1, tx_count):
            tx = self._get_dummy_tx(i)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            tx_hash = hashlib.sha256(tx_bytes)

            self.tx_service.assign_tx_to_sid(tx_hash, i, time.time())
            self.tx_service.hash_to_contents[tx_hash] = tx_bytes

            short_tx = ShortTransaction(0, i, bytes())
            short_txs.append(short_tx)

        dummy_chain_difficulty = 2000000

        compact_block = CompactBlock(self._get_dummy_block_header(7),
                                     short_txs,
                                     [
                                         self._get_dummy_block_header(2),
                                         self._get_dummy_block_header(3)
                                     ],
                                     dummy_chain_difficulty)

        compact_block_bytes = rlp.encode(compact_block, CompactBlock)
        compact_block_hash_bytes = hashlib.sha256(compact_block_bytes).digest()
        compact_block_hash = ObjectHash(compact_block_hash_bytes)

        bx_block_msg = BroadcastMessage(compact_block_hash, self.test_network_num, compact_block_bytes)

        block_msg, block_hash, unknown_tx_sids, unknown_tx_hashes = self.message_parser.bx_block_to_block(
            bx_block_msg.blob(), self.tx_service)

        self.assertTrue(block_msg)
        self.assertIsInstance(block_msg, NewBlockEthProtocolMessage)
        self.assertEqual(block_msg.block_hash(), block_hash)

        block = block_msg.get_block()

        self._assert_values_equal(compact_block.header, block.header)
        self._assert_values_equal(compact_block.uncles, block.uncles)

        self.assertEqual(len(compact_block.transactions), len(block.transactions))

        for block_tx, i in zip(block.transactions, range(0, tx_count - 1)):
            self.assertIsInstance(block_tx, Transaction)

            self._assert_values_equal(block_tx, txs[i])

        self.assertEqual(compact_block.chain_difficulty, block_msg.chain_difficulty)

    def test_bx_block_to_block__full_txs_success(self):
        tx_count = 10
        txs = []
        short_txs = []

        for i in range(1, tx_count):
            tx = self._get_dummy_tx(i)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)

            if i % 2 == 0:
                tx_hash = hashlib.sha256(tx_bytes)

                self.tx_service.assign_tx_to_sid(tx_hash, i, time.time())
                self.tx_service.hash_to_contents[tx_hash] = tx_bytes

                short_tx = ShortTransaction(0, i, bytes())
                short_txs.append(short_tx)
            else:
                short_tx = ShortTransaction(1, 0, tx_bytes)
                short_txs.append(short_tx)

        dummy_chain_difficulty = 20

        compact_block = CompactBlock(self._get_dummy_block_header(8),
                                     short_txs,
                                     [
                                         self._get_dummy_block_header(2),
                                         self._get_dummy_block_header(3)
                                     ],
                                     dummy_chain_difficulty)

        compact_block_bytes = rlp.encode(compact_block, CompactBlock)
        compact_block_hash_bytes = hashlib.sha256(compact_block_bytes).digest()
        compact_block_hash = ObjectHash(compact_block_hash_bytes)

        bx_block_msg = BroadcastMessage(compact_block_hash, self.test_network_num, compact_block_bytes)

        block_msg, block_hash, unknown_tx_sids, unknown_tx_hashes = self.message_parser.bx_block_to_block(
            bx_block_msg.blob(), self.tx_service)

        self.assertTrue(block_msg)
        self.assertIsInstance(block_msg, NewBlockEthProtocolMessage)

        block = block_msg.get_block()

        self._assert_values_equal(compact_block.header, block.header)
        self._assert_values_equal(compact_block.uncles, block.uncles)

        self.assertEqual(len(compact_block.transactions), len(block.transactions))

        for block_tx, i in zip(block.transactions, range(0, tx_count - 1)):
            self.assertIsInstance(block_tx, Transaction)

            self._assert_values_equal(block_tx, txs[i])

        self.assertEqual(compact_block.chain_difficulty, block_msg.chain_difficulty)

    def test_block_to_bx_block_then_bx_block_to_block__success(self):
        txs = []
        txs_bytes = []
        txs_hashes = []
        short_ids = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = self._get_dummy_tx(i)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

            self.tx_service.assign_tx_to_sid(tx_hash, i, time.time())
            self.tx_service.hash_to_contents[tx_hash] = tx_bytes
            short_ids.append(i)

        block = Block(self._get_dummy_block_header(100), txs, [self._get_dummy_block_header(2)])

        dummy_chain_difficulty = 40000000

        block_msg = NewBlockEthProtocolMessage(None, block, dummy_chain_difficulty)
        block_msg_bytes = block_msg.rawbytes()
        self.assertTrue(block_msg_bytes)

        bx_block_msg = self.message_parser.block_to_bx_block(block_msg, self.tx_service)
        self.assertIsNotNone(bx_block_msg)

        converted_block_msg, _, _, _ = self.message_parser.bx_block_to_block(bx_block_msg, self.tx_service)
        self.assertIsNotNone(converted_block_msg)

        converted_block_msg_bytes = converted_block_msg.rawbytes()

        self.assertEqual(len(converted_block_msg_bytes), len(block_msg_bytes))
        self.assertEqual(converted_block_msg_bytes, block_msg_bytes)

    def _get_dummy_tx(self, nonce):
        # create transaction object with dummy values multiplied by nonce to be able generate txs with different values
        return Transaction(
            nonce,
            2 * nonce,
            3 * nonce,
            helpers.generate_bytearray(eth_constants.ADDRESS_LEN),
            4 * nonce,
            helpers.generate_bytearray(15 * nonce),
            5 * nonce,
            6 * nonce,
            7 * nonce)

    def _get_dummy_block_header(self, nonce):
        # create BlockHeader object with dummy values multiplied by nonce to be able generate txs with different value
        return BlockHeader(
            helpers.generate_bytearray(eth_constants.BLOCK_HASH_LEN),
            helpers.generate_bytearray(eth_constants.BLOCK_HASH_LEN),
            helpers.generate_bytearray(eth_constants.ADDRESS_LEN),
            helpers.generate_bytearray(eth_constants.MERKLE_ROOT_LEN),
            helpers.generate_bytearray(eth_constants.MERKLE_ROOT_LEN),
            helpers.generate_bytearray(eth_constants.MERKLE_ROOT_LEN),
            100 * nonce,
            nonce,
            2 * nonce,
            3 * nonce,
            4 * nonce,
            5 * nonce,
            helpers.generate_bytearray(100 * nonce),
            helpers.generate_bytearray(eth_constants.BLOCK_HASH_LEN),
            helpers.generate_bytearray(nonce)
        )

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