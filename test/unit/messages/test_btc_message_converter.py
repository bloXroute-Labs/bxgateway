import functools
import os
import random
from argparse import Namespace
from collections import defaultdict

import task_pool_executor as tpe
from bxcommon.models.transaction_info import TransactionInfo
from mock import MagicMock

import bxgateway.messages.btc.btc_message_converter_factory as converter_factory
from bxcommon.constants import NULL_TX_SID, USE_EXTENSION_MODULES
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import convert
from bxcommon.utils import crypto
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


def with_extensions(func):
    @functools.wraps(func)
    def run_with_extensions(self, *args, **kwargs):
        try:
            self._enable_extensions()
            func(self, *args, **kwargs)
        finally:
            self._restore_extensions_flag()

    return run_with_extensions


class BtcMessageConverterTests(AbstractTestCase):
    AVERAGE_TX_SIZE = 250
    MAGIC = 123

    def setUp(self):
        self.opts = Namespace()
        self.opts.use_extensions = USE_EXTENSION_MODULES
        self.btc_message_converter = converter_factory.create_btc_message_converter(self.MAGIC, opts=self.opts)

    def test_tx_msg_to_btc_tx_msg__success(self):
        tx_hash = Sha256Hash(helpers.generate_bytearray(SHA256_HASH_LEN))
        tx = helpers.generate_bytearray(self.AVERAGE_TX_SIZE)

        tx_msg = TxMessage(tx_hash=tx_hash, network_num=12345, tx_val=tx)

        btc_tx_msg = self.btc_message_converter.bx_tx_to_tx(tx_msg)

        self.assertTrue(btc_tx_msg)
        self.assertEqual(btc_tx_msg.magic(), self.MAGIC)
        self.assertEqual(btc_tx_msg.command(), b"tx")
        self.assertEqual(btc_tx_msg.payload(), tx)

    def test_tx_msg_to_btc_tx_msg__type_error(self):
        btc_tx_msg = TxBtcMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))

        self.assertRaises(TypeError, self.btc_message_converter.bx_tx_to_tx, btc_tx_msg)

    def test_btc_tx_msg_to_tx_msg__success(self):
        btc_tx_msg = TxBtcMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))
        network_num = 12345

        tx_msgs = self.btc_message_converter.tx_to_bx_txs(btc_tx_msg, network_num)

        self.assertTrue(tx_msgs)
        self.assertIsInstance(tx_msgs[0][0], TxMessage)
        self.assertEqual(tx_msgs[0][0].network_num(), network_num)
        self.assertEqual(tx_msgs[0][1], btc_tx_msg.tx_hash())
        self.assertEqual(tx_msgs[0][2], btc_tx_msg.tx())

    def test_btc_tx_msg_to_tx_msg__type_error(self):
        tx_msg = TxMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))

        self.assertRaises(TypeError, self.btc_message_converter.tx_to_bx_txs, tx_msg)

    def test_btc_block_to_bloxroute_block_and_back_sids_found(self):
        magic = 12345
        version = 23456
        prev_block_hash = bytearray(crypto.bitcoin_hash(b"123"))
        prev_block = BtcObjectHash(prev_block_hash, length=SHA256_HASH_LEN)
        merkle_root_hash = bytearray(crypto.bitcoin_hash(b"234"))
        merkle_root = BtcObjectHash(merkle_root_hash, length=SHA256_HASH_LEN)
        timestamp = 1
        bits = 2
        nonce = 3

        txns = [TxBtcMessage(magic, version, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in range(10)]
        txn_hashes = list(map(lambda x: BtcObjectHash(buf=crypto.bitcoin_hash(x), length=SHA256_HASH_LEN), txns))
        short_ids = [i for i in range(1, 6)]

        btc_block = BlockBtcMessage(magic, version, prev_block, merkle_root, timestamp, bits, nonce, txns)
        block_hash = btc_block.block_hash()

        # find an sid for half the transactions
        def get_short_id(txhash):
            index = txn_hashes.index(txhash)
            if index % 2 == 0:
                return short_ids[int(index / 2)]
            else:
                return NULL_TX_SID

        # return a transaction's info for assigned sids
        def get_transaction(sid):
            return TransactionInfo(txn_hashes[(sid - 1) * 2], txns[(sid - 1) * 2], sid)

        tx_service = MagicMock()
        tx_service.get_short_id = get_short_id
        tx_service.cpp_tx_hash_to_short_ids = tpe.Sha256ToShortIDMap()
        for index, tx_hash in enumerate(txn_hashes):
            short_id = get_short_id(tx_hash)
            if short_id == NULL_TX_SID:
                continue
            sha = tpe.Sha256(tpe.InputBytes(tx_hash.binary))
            tx_service.cpp_tx_hash_to_short_ids[sha] = short_id

        tx_service.get_transaction = get_transaction

        bloxroute_block, block_info = self.btc_message_converter.block_to_bx_block(btc_block, tx_service)
        self.assertEqual(10, block_info.txn_count)
        self.assertEqual("5a77d1e9612d350b3734f6282259b7ff0a3f87d62cfef5f35e91a5604c0490a3",
                         block_info.prev_block_hash)
        self.assertEqual(short_ids, list(block_info.short_ids))

        # TODO: if we convert bloxroute block to a class, add some tests here

        parsed_btc_block, block_info, _, _ = self.btc_message_converter.bx_block_to_block(
            bloxroute_block,
            tx_service)
        self.assertIsNotNone(block_info)
        self.assertEqual(version, parsed_btc_block.version())
        self.assertEqual(magic, parsed_btc_block.magic())
        self.assertEqual(prev_block_hash, parsed_btc_block.prev_block().get_little_endian())
        self.assertEqual(merkle_root_hash, parsed_btc_block.merkle_root().get_little_endian())
        self.assertEqual(timestamp, parsed_btc_block.timestamp())
        self.assertEqual(bits, parsed_btc_block.bits())
        self.assertEqual(nonce, parsed_btc_block.nonce())
        self.assertEqual(len(txns), parsed_btc_block.txn_count())
        self.assertEqual(btc_block.checksum(), parsed_btc_block.checksum())
        self.assertEqual(block_hash, parsed_btc_block.block_hash())
        self.assertEqual(block_hash.binary, block_info.block_hash.binary)
        self.assertEqual(block_info.short_ids, short_ids)

    @with_extensions
    def test_plain_compression(self):
        tx_service, parsed_block, btc_message_converter = self._init()
        ref_block, ref_block_info = self.btc_message_converter.block_to_bx_block(parsed_block, tx_service)
        bx_block, block_info = btc_message_converter.block_to_bx_block(parsed_block, tx_service)
        self.assertEqual(bx_block, ref_block)

    @with_extensions
    def test_partial_compression(self):
        tx_service, parsed_block, btc_message_converter = self._init()
        transactions_short = parsed_block.txns()[:]
        random.shuffle(transactions_short)
        transactions_short = transactions_short[:int(len(transactions_short) * 0.9)]
        for short_id, txn in enumerate(transactions_short):
            bx_tx_hash = BtcObjectHash(buf=crypto.double_sha256(txn),
                                       length=BTC_SHA_HASH_LEN)
            tx_service.assign_short_id(bx_tx_hash, short_id + 1)
        bx_block, block_info = btc_message_converter.block_to_bx_block(parsed_block, tx_service)
        ref_block, _ = self.btc_message_converter.block_to_bx_block(parsed_block, tx_service)
        self.assertEqual(bx_block, ref_block)

    def _enable_extensions(self):
        self.opts.use_extensions = True

    def _restore_extensions_flag(self):
        self.opts.use_extensions = USE_EXTENSION_MODULES

    def _init(self):
        root_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(root_dir, "sample_block.txt")) as sample_file:
            btc_block = sample_file.read().strip("\n")
        magic = "12345"
        tx_service = MagicMock()
        tx_service._tx_hash_to_short_ids = defaultdict(set)
        tx_service.cpp_tx_hash_to_short_ids = tpe.Sha256ToShortIDMap()

        def get_short_id(tx_hash):
            if tx_hash in tx_service._tx_hash_to_short_ids:
                return tx_service._tx_hash_to_short_ids[tx_hash]
            else:
                return NULL_TX_SID

        def assign_short_id(tx_hash, short_id):
            tx_service._tx_hash_to_short_ids[tx_hash] = short_id
            cpp_hash = tpe.Sha256(tpe.InputBytes(tx_hash.binary))
            tx_service.cpp_tx_hash_to_short_ids[cpp_hash] = short_id

        tx_service.get_short_id = get_short_id
        tx_service.assign_short_id = assign_short_id
        btc_message_converter = converter_factory.create_btc_message_converter(magic, self.opts)
        buf = bytearray(convert.hex_to_bytes(btc_block))
        parsed_block = BlockBtcMessage(buf=buf)
        return tx_service, parsed_block, btc_message_converter
