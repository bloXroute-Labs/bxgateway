import functools
import os
import random
from argparse import Namespace

import bxgateway.messages.btc.btc_message_converter_factory as converter_factory
from bxcommon.constants import LOCALHOST
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.utils import convert
from bxcommon.utils import crypto
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.services.transaction_service import TransactionService
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


def get_sample_block():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(root_dir, "sample_block.txt")) as sample_file:
        btc_block = sample_file.read().strip("\n")
    buf = bytearray(convert.hex_to_bytes(btc_block))
    parsed_block = BlockBtcMessage(buf=buf)
    return parsed_block


class multi_setup(object):
    def __init__(self, txns=False):
        self.txns = txns

    def __call__(self, func):
        @functools.wraps(func)
        def run_multi_setup(instance):
            if self.txns:
                instance.txns = [
                    TxBtcMessage(
                        instance.magic,
                        instance.version,
                        [],
                        [],
                        i
                    ).rawbytes()[BTC_HDR_COMMON_OFF:] for i in range(10)
                ]
            else:
                instance.txns = []
            normal_tx_service, normal_converter = instance.init(False)
            extension_tx_service, extension_converter = instance.init(True)
            instance.btc_message_converter = normal_converter
            instance.tx_service = normal_tx_service
            func(instance)
            instance.btc_message_converter = extension_converter
            instance.tx_service = extension_tx_service
            func(instance)
        return run_multi_setup


class BtcMessageConverterTests(AbstractTestCase):
    AVERAGE_TX_SIZE = 250
    MAGIC = 123

    def setUp(self):
        self.btc_message_converter = None
        self.tx_service = None
        self.txns = []
        self.test_data = None
        self.short_ids = [i for i in range(1, 6)]
        self.magic = 12345
        self.version = 23456

    @multi_setup()
    def test_tx_msg_to_btc_tx_msg__success(self):
        tx_hash = Sha256Hash(helpers.generate_bytearray(SHA256_HASH_LEN))
        tx = helpers.generate_bytearray(self.AVERAGE_TX_SIZE)

        tx_msg = TxMessage(tx_hash=tx_hash, network_num=12345, tx_val=tx)

        btc_tx_msg = self.btc_message_converter.bx_tx_to_tx(tx_msg)

        self.assertTrue(btc_tx_msg)
        self.assertEqual(btc_tx_msg.magic(), self.MAGIC)
        self.assertEqual(btc_tx_msg.command(), b"tx")
        self.assertEqual(btc_tx_msg.payload(), tx)

    @multi_setup()
    def test_tx_msg_to_btc_tx_msg__type_error(self):
        btc_tx_msg = TxBtcMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))

        self.assertRaises(TypeError, self.btc_message_converter.bx_tx_to_tx, btc_tx_msg)

    @multi_setup()
    def test_btc_tx_msg_to_tx_msg__success(self):
        btc_tx_msg = TxBtcMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))
        network_num = 12345

        tx_msgs = self.btc_message_converter.tx_to_bx_txs(btc_tx_msg, network_num)

        self.assertTrue(tx_msgs)
        self.assertIsInstance(tx_msgs[0][0], TxMessage)
        self.assertEqual(tx_msgs[0][0].network_num(), network_num)
        self.assertEqual(tx_msgs[0][1], btc_tx_msg.tx_hash())
        self.assertEqual(tx_msgs[0][2], btc_tx_msg.tx())

    @multi_setup()
    def test_btc_tx_msg_to_tx_msg__type_error(self):
        tx_msg = TxMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))

        self.assertRaises(TypeError, self.btc_message_converter.tx_to_bx_txs, tx_msg)

    @multi_setup(True)
    def test_btc_block_to_bloxroute_block_and_back_sids_found(self):
        prev_block_hash = bytearray(crypto.bitcoin_hash(b"123"))
        prev_block = BtcObjectHash(prev_block_hash, length=SHA256_HASH_LEN)
        merkle_root_hash = bytearray(crypto.bitcoin_hash(b"234"))
        merkle_root = BtcObjectHash(merkle_root_hash, length=SHA256_HASH_LEN)
        timestamp = 1
        bits = 2
        nonce = 3

        btc_block = BlockBtcMessage(self.magic, self.version, prev_block, merkle_root, timestamp, bits, nonce, self.txns)
        block_hash = btc_block.block_hash()

        bloxroute_block, block_info = self.btc_message_converter.block_to_bx_block(btc_block, self.tx_service)
        self.assertEqual(10, block_info.txn_count)
        self.assertEqual("5a77d1e9612d350b3734f6282259b7ff0a3f87d62cfef5f35e91a5604c0490a3",
                         block_info.prev_block_hash)
        self.assertEqual(self.short_ids, list(block_info.short_ids))
        self.assertEqual(btc_block.block_hash(), block_info.block_hash)

        # TODO: if we convert bloxroute block to a class, add some tests here

        parsed_btc_block, block_info, _, _ = self.btc_message_converter.bx_block_to_block(
            bloxroute_block,
            self.tx_service)
        self.assertIsNotNone(block_info)
        self.assertEqual(parsed_btc_block.rawbytes().tobytes(), btc_block.rawbytes().tobytes())
        self.assertEqual(self.version, parsed_btc_block.version())
        self.assertEqual(self.magic, parsed_btc_block.magic())
        self.assertEqual(prev_block_hash, parsed_btc_block.prev_block().get_little_endian())
        self.assertEqual(merkle_root_hash, parsed_btc_block.merkle_root().get_little_endian())
        self.assertEqual(timestamp, parsed_btc_block.timestamp())
        self.assertEqual(bits, parsed_btc_block.bits())
        self.assertEqual(nonce, parsed_btc_block.nonce())
        self.assertEqual(len(self.txns), parsed_btc_block.txn_count())
        self.assertEqual(btc_block.checksum(), parsed_btc_block.checksum())
        self.assertEqual(block_hash, parsed_btc_block.block_hash())
        self.assertEqual(block_hash.binary, block_info.block_hash.binary)
        self.assertEqual(list(block_info.short_ids), self.short_ids)

    @multi_setup()
    def test_plain_compression(self):
        parsed_block = get_sample_block()
        bx_block, bx_block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service)
        ref_block, block_info, _, _ = self.btc_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )
        self.assertEqual(
            parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes()
        )

    @multi_setup()
    def test_partial_compression(self):
        parsed_block = get_sample_block()
        transactions_short = parsed_block.txns()[:]
        random.shuffle(transactions_short)
        transactions_short = transactions_short[:int(len(transactions_short) * 0.9)]
        for short_id, txn in enumerate(transactions_short):
            bx_tx_hash = BtcObjectHash(buf=crypto.double_sha256(txn),
                                       length=BTC_SHA_HASH_LEN)
            self.tx_service.assign_short_id(bx_tx_hash, short_id + 1)
            self.tx_service.set_transaction_contents(bx_tx_hash, txn)
        bx_block, block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service)
        ref_block, _, _, _ = self.btc_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )
        self.assertEqual(
            parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes()
        )

    def init(self, use_extensions: bool):
        opts = Namespace()
        opts.use_extensions = use_extensions
        opts.import_extensions = use_extensions
        btc_message_converter = converter_factory.create_btc_message_converter(self.MAGIC, opts=opts)
        if use_extensions:
            tx_service = ExtensionTransactionService(MockNode(LOCALHOST, 8999), 0)
        else:
            tx_service = TransactionService(MockNode(LOCALHOST, 8999), 0)
        if self.txns:
            for idx, txn in enumerate(self.txns):
                sha = BtcObjectHash(buf=crypto.bitcoin_hash(txn), length=SHA256_HASH_LEN)
                if idx % 2 == 0:
                    tx_service.assign_short_id(sha, self.short_ids[int(idx/2)])
                    tx_service.set_transaction_contents(sha, txn)
        return tx_service, btc_message_converter
