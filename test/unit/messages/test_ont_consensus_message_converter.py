import functools
import os
import random
import time
from argparse import Namespace

from bxgateway.testing import gateway_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.constants import DEFAULT_TX_MEM_POOL_BUCKET_SIZE
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.utils import convert
from bxcommon.services.transaction_service import TransactionService
from bxcommon.services.extension_transaction_service import ExtensionTransactionService

import bxgateway.messages.ont.ont_consensus_message_converter_factory as converter_factory
from bxgateway.messages.ont.abstract_ont_message_converter import AbstractOntMessageConverter
from bxgateway.messages.ont.consensus_ont_message import OntConsensusMessage
from bxgateway.messages.ont import ont_messages_util


def get_sample_block():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root_dir, "samples/ont_consensus_sample_block.txt")) as sample_file:
        ont_block = sample_file.read().strip("\n")
    buf = bytearray(convert.hex_to_bytes(ont_block))
    parsed_block = OntConsensusMessage(buf=buf)
    return parsed_block


class multi_setup:
    def __call__(self, func):
        @functools.wraps(func)
        def run_multi_setup(instance):
            normal_tx_service, normal_converter = instance.init(False)
            extension_tx_service, extension_converter = instance.init(True)
            instance.ont_message_converter = normal_converter
            instance.tx_service = normal_tx_service
            func(instance)
            instance.ont_message_converter = extension_converter
            instance.tx_service = extension_tx_service
            func(instance)
        return run_multi_setup


class OntMessageConverterTests(AbstractTestCase):
    MAGIC = 123
    SAMPLE_BLOCK_PREV_BLOCK_HASH = "15c601ff83641e964b26930b6ade245fb92c6cec18945a19a4b8f3298d0d3cd4"
    SAMPLE_BLOCK_BLOCK_HASH = "f15a4899c071b38c1a37707252cc4b198c15b53be811c777808317cba365085f"
    SAMPLE_BLOCK_TX_COUNT = 17

    def setUp(self):
        self.ont_message_converter: AbstractOntMessageConverter = None
        self.tx_service = None
        self._prev_bx_block = None
        self._prev_bx_block_info = None

    @multi_setup()
    def test_plain_compression(self):
        parsed_block = get_sample_block()
        bx_block, bx_block_info = self.ont_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
        if self._prev_bx_block is not None:
            self.assertEqual(bytearray(self._prev_bx_block), bytearray(bx_block), "raw block")
            self.assertEqual(len(self._prev_bx_block_info.short_ids), len(bx_block_info.short_ids), "short_ids")
            self.assertEqual(self._prev_bx_block_info.txn_count, bx_block_info.txn_count, "txn_count")
            self.assertEqual(self._prev_bx_block_info.prev_block_hash, bx_block_info.prev_block_hash, "prev_block_hash")
            self.assertEqual(self._prev_bx_block_info.compressed_block_hash, bx_block_info.compressed_block_hash, "compressed_block_hash")
        ref_block, block_info, _, _ = self.ont_message_converter.bx_block_to_block(bx_block, self.tx_service)
        self.assertEqual(parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes())
        self.assertEqual(self.SAMPLE_BLOCK_TX_COUNT, parsed_block.txn_count())
        self.assertEqual(self.SAMPLE_BLOCK_TX_COUNT, ref_block.txn_count())
        self.assertEqual(bytearray(convert.hex_to_bytes(self.SAMPLE_BLOCK_PREV_BLOCK_HASH)),
                         parsed_block.prev_block_hash().get_little_endian())
        self.assertEqual(bytearray(convert.hex_to_bytes(self.SAMPLE_BLOCK_PREV_BLOCK_HASH)),
                         ref_block.prev_block_hash().get_little_endian())
        self.assertEqual(bytearray(convert.hex_to_bytes(self.SAMPLE_BLOCK_BLOCK_HASH)),
                         parsed_block.block_hash().get_little_endian())
        self.assertEqual(bytearray(convert.hex_to_bytes(self.SAMPLE_BLOCK_BLOCK_HASH)),
                         ref_block.block_hash().get_little_endian())
        self._prev_bx_block = bx_block
        self._prev_bx_block_info = bx_block_info

    @multi_setup()
    def test_partial_compression(self):
        parsed_block = get_sample_block()
        transactions_short = parsed_block.txns()[:]
        random.shuffle(transactions_short)
        transactions_short = transactions_short[:int(len(transactions_short) * 0.9)]
        for short_id, txn in enumerate(transactions_short):
            bx_tx_hash, _ = ont_messages_util.get_txid(txn)
            transaction_key = self.tx_service.get_transaction_key(bx_tx_hash)
            self.tx_service.assign_short_id_by_key(transaction_key, short_id + 1)
            self.tx_service.set_transaction_contents_by_key(transaction_key, txn)
        bx_block, block_info = self.ont_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
        ref_block, _, unknown_tx_sids, unknown_tx_hashes = self.ont_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )
        self.assertEqual(len(unknown_tx_hashes), 0)
        self.assertEqual(len(unknown_tx_sids), 0)
        self.assertEqual(
            parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes()
        )

    @multi_setup()
    def test_full_compression(self):
        parsed_block = get_sample_block()
        transactions = parsed_block.txns()[:]
        random.shuffle(transactions)
        for short_id, txn in enumerate(transactions):
            bx_tx_hash, _ = ont_messages_util.get_txid(txn)
            self.tx_service.assign_short_id(bx_tx_hash, short_id + 1)
            self.tx_service.set_transaction_contents(bx_tx_hash, txn)
        bx_block, block_info = self.ont_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
        ref_block, ref_block_info, unknown_tx_sids, unknown_tx_hashes = self.ont_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )
        self.assertEqual(len(block_info.short_ids), block_info.txn_count, "all txs were compressed")
        self.assertEqual(len(unknown_tx_hashes), 0)
        self.assertEqual(len(unknown_tx_sids), 0)
        self.assertEqual(
            parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes()
        )

    def init(self, use_extensions: bool):
        opts = Namespace()
        opts.use_extensions = use_extensions
        opts.import_extensions = use_extensions
        opts.tx_mem_pool_bucket_size = DEFAULT_TX_MEM_POOL_BUCKET_SIZE
        ont_message_converter = converter_factory.create_ont_consensus_message_converter(self.MAGIC, opts)
        if use_extensions:
            helpers.set_extensions_parallelism()
            tx_service = ExtensionTransactionService(MockNode(
                gateway_helpers.get_gateway_opts(8999)), 0)
        else:
            tx_service = TransactionService(MockNode(
                gateway_helpers.get_gateway_opts(8999)), 0)
        return tx_service, ont_message_converter
