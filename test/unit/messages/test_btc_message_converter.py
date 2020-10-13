import functools
import os
import random
from argparse import Namespace

from bxgateway.testing import gateway_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.constants import DEFAULT_TX_MEM_POOL_BUCKET_SIZE
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.utils import convert, crypto
from bxcommon.utils.blockchain_utils.btc import btc_common_utils
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.services.transaction_service import TransactionService
from bxcommon.services.extension_transaction_service import ExtensionTransactionService

import bxgateway.messages.btc.btc_message_converter_factory as converter_factory
from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage, BtcMessage
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.testing.mocks import mock_btc_messages

COMPACT_BLOCK_BYTES_HEX = "dab5bffa636d706374626c6f636b000003010000eb318d3e0000002011bf3e2bd32bfa7393f12481053311721563a44b2d70f82d016892d8ddc1bf68abce352f93a8863c211964ba40440b98d85bd45765904c31f08f2489afcb9eadca67dc5cffff7f20010000009c93473e3cd9419e0aa51da22d1277f6029f689d84f33b55f255f74a02dca40e69616cfcb4c2363ea0f7ce1115b1f26bb52f3ad616eeb21f0712f57c414c07693fb416451c010002000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0d01660101082f454233322e302fffffffff012afe052a01000000232102a0f2d61a2b8cf7dc750d36914e4834ec6f5bb5a7f9979305ae4a687c3a152626ac00000000"
FULL_BLOCK_BYTES_HEX = "dab5bffa626c6f636b00000000000000e40c00008a8332a90000002011bf3e2bd32bfa7393f12481053311721563a44b2d70f82d016892d8ddc1bf68abce352f93a8863c211964ba40440b98d85bd45765904c31f08f2489afcb9eadca67dc5cffff7f20010000000b02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0d01660101082f454233322e302fffffffff012afe052a01000000232102a0f2d61a2b8cf7dc750d36914e4834ec6f5bb5a7f9979305ae4a687c3a152626ac000000000200000002b08b1876f5d8cee83f6c418adea81899eccd1279a3924c504b3a4139f76e6564010000006b483045022100f46036bd6f2dce62af9fd849aee12b934956a1992f10ef40dfbb02247fac92c902206bdacfc5ef2737db5c491c32cffd1fa3afb2a823fd5b0d2b5c7fab6afeb2aead412103ca689547cff4c5d764906d26499c1454395a513f20dea9c099f660b3dd3f0402feffffff2babfe9c3f4e8c65e7e5e856c61100522fca7eb691d12984d6a61afe1f3b2eed010000006b4830450221008b9b550701f512c5e7d3d0e0d1665ea93548b3f412d4482115555184601cdbd70220196da0d2d32756e4b062ea7023d1a0ebbe9412708a8ac6c06f45999338a18c6d412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffff0280969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188aca8909800000000001976a914ed40d7d846a8d6fcc33276dbf81c477d110b23c688ac6500000002000000012babfe9c3f4e8c65e7e5e856c61100522fca7eb691d12984d6a61afe1f3b2eed000000006b483045022100f84fa0ff6a87b3a031dfccc98521f841b210456c96bf25221872037c5281c27a0220191ee197092177637f1e224a8239f5f504e1d2f72d8041430b904e9021dcc620412103ad859644a3f56390f28d25b20fdfb37209705b1d08d62966ab5676baffbdf27afeffffff0280969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac9a94a327010000001976a914c4b1f46dbad7dfbef87a6850da4db77f613647d488ac650000000200000002a82bbe8c4213a31b21abc006c9317134f91c0a91a72fe1189d4601497aa306b2000000006b483045022100aeea1993a6da728d90361194540b53769e0a96a1bbf1192410c9239f3b30505802207b44c4be08dfc4e6d6c75e5578c22a698ef8fee595ce1c51d3e017235cd50a5d412102543c8dda542a4fa17092b7c63ccadae0aad0dbf92e88eb55b9fe37d9d75e85cafeffffffa82bbe8c4213a31b21abc006c9317134f91c0a91a72fe1189d4601497aa306b2010000006a47304402204b1fe7a9de4ab7fe0be0fe8568f0c7e8eefc0f6dce948d91e7bddd4e41977e0102202c6961ff0b8e8e57e07546f472ffed8ddc36fac939e0f771b0d3ea896151b9c2412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffff0280969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac1e929800000000001976a914dc1d1a993b4aa3bc7406d6da463a0b2f71c152df88ac5a00000002000000025132c4f4152b1cd080f306c75771fdcb730d75a3fbe2e18a0a5184b71e66e917000000006b483045022100e004f686ffd4352a40dfeaef1498f9f3a595a277d4f644bd74588970a5a47427022002692b57834fb2421882c986b12038c930adfadd3d778ab17005e28898935f4f412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffffd88ad4ae87a0f5d57dc480aad5c8aaf37aba784f43a92af786bfbd6fb46b46f2010000006b483045022100c403571ca4cee795afbe185e77b6cfc1e4e1a90d4f8a065c137e1f8f4c3de46a0220340fc089fb372f5c3317576c409d992cc00921a5cd2e9d062151482cf415ab2b412103d04329b7dc3d8e5651c1fb739e37e4d3773956b4004440c5bed864709532995dfeffffff02bc8d9800000000001976a9149544931a7370dd88d4087e60a3eac989c2b228f488ac80969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac650000000200000002683ddb1968c1b821664c0972058ed391f3d6e9f5d5d0d46371d3a5401707f1e6000000006b483045022100b75241634770e02de2cbcc9d87be3e95fc9496d6381ccbbd423a2aa571d8946a0220361ba4f3504a331bb96858927505f98d163856d913913998d128b72926ff82db412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffff683ddb1968c1b821664c0972058ed391f3d6e9f5d5d0d46371d3a5401707f1e6010000006b483045022100e1eebd30ff210c548070b6aebe23588c9a5c711969c34cc3355ab038654a948f022005cd1272c3b93056e30e5a5c67664dc77aa0defe9c430f150e072ac539f14aca412102be1bd1a77c58b1214af15acfd29c4e015a70ed9adab8ce9f21a532b4edc47256feffffff0294939800000000001976a914fd46648414af50287f237690eae844b0ae7a9e5888ac80969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac510000000200000001958604265d8507c3fc8230f2fa9fb2c4d31bab6535912c1257ecc1e26db530e7010000006a473044022030fbc8ff3bc1f102d8a2964d96fbcf1c5cc3d3f725488efdecd6c8d40b97626802207885b7d5989ba2eaab2a7e65cd83e823ac3a343341832a00cbd8b760e37db018412103f60cc4ecab76e110469253e628bf66c4fc517c0a97682357f38244bd7b942145feffffff025ec3d428010000001976a9144ffb61739f9781b05b56666d72c1a28997c880db88ac80969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac650000000200000002a0ca521ced8e00bdbf063921b0c9d190145bbb98c767084e9bdd7344edc6dbc6010000006a4730440220634d204299971dbe1974676579c169a7a9fadc4bf8ad0a70345890524b61d53b02201e39c48784d95444ad07190644acfb6182521d9a15344b09d5e054a978a837bb412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffff958604265d8507c3fc8230f2fa9fb2c4d31bab6535912c1257ecc1e26db530e7000000006b483045022100c730f6870c8cc9a4169fb95f39084fd5b03b8802e3beca3e1682e4cf88ee27c102201c5be02d4009419e8d326c340259b73fc8820179f093ff9aabdc81b184bf7d3a412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffff0280969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac0a959800000000001976a914cdd4a4d97ded815b33daf7c04c29f6876ba5447988ac650000000200000001ccc1b706ad44c0463a72623a6008f8b72fae2931643a3125a1521506042f96af0000000049483045022100cbea44677352a1c4c58ae57ac8f47cc2296892f0b63a25a862239f788a1e9968022033835258c79ad8ec118f3ecddc755b101f074057ae93e51f9ecd4d1cb8a44c3541feffffff0280969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188acc05a6d29010000001976a914b30b2534ef74db0aa962bbb799ae3954b04e642088ac650000000200000001a0ca521ced8e00bdbf063921b0c9d190145bbb98c767084e9bdd7344edc6dbc6000000006b483045022100c2c2ce251e70b18b5cb43dd3ecc825ffeeaaee4a54094d69ffc46eaceed0fee802204b644caea2cde782a4d382fc1e505205075ca1f2c8547d31ac2d3f1bc812c8ea41210286352d399cebed78ab865cdda48f9100d560a2b40cc192d8303a70e53cafcc0ffeffffff02fc2b3c28010000001976a91467e598da3dfdc7db230a47fef58f1d4d71bac26388ac80969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac6500000002000000025132c4f4152b1cd080f306c75771fdcb730d75a3fbe2e18a0a5184b71e66e917010000006b48304502210082783e18a441783a01d73b1ff4903c289dfe60f87faebc9022202d820fc55c9802203483e0d0a36d57f5909da1dcfa32578829c74fc57a0e3b02d52087ed9cfe2bbb41210249c38cc355cc1b0f839e805e8616cd0ad33e00f7564ea301fe5def73f58bd7b9feffffffbe042cdfd92458062d9f65c075cc77b49b1f9cdf0e8fb1853c2e5a6357bdf424000000006b483045022100a0b506171a668487cad6fd84a05e741426dbfedf41d3484fbac92956c9f8225b022020bac4daac9a107f03880acbdc7bb9e2fdd976a7c6352924cd534383a7d95549412102551bf1afa64550774331b8c157d5c4cedc424f2d74d016f05c2b86d6493f9157feffffff0280969800000000001976a9144c42b323d77e7ae2f3d88294a5557c78eee292a188ac328f9800000000001976a91496f8161d8758b78dd07a8683141b80fb5155a30b88ac65000000"


def get_sample_block():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root_dir, "samples/btc_sample_block.txt")) as sample_file:
        btc_block = sample_file.read().strip("\n")
    buf = bytearray(convert.hex_to_bytes(btc_block))
    parsed_block = BlockBtcMessage(buf=buf)
    return parsed_block


def get_segwit_block():
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root_dir, "samples/btc_segwit_sample_block.txt")) as sample_file:
        btc_block = sample_file.read().strip("\n")
    block = convert.hex_to_bytes(btc_block)
    buf = bytearray(BTC_HDR_COMMON_OFF + len(block))
    buf[BTC_HDR_COMMON_OFF:] = block
    msg = BtcMessage(magic="main", command=BlockBtcMessage.MESSAGE_TYPE, payload_len=len(block), buf=buf)
    parsed_block = BlockBtcMessage(buf=msg.buf)
    return parsed_block


def get_sample_compact_block():
    buf = bytearray(convert.hex_to_bytes(COMPACT_BLOCK_BYTES_HEX))
    parsed_block = CompactBlockBtcMessage(buf=buf)
    return parsed_block


def get_recovered_compact_block():
    buf = bytearray(convert.hex_to_bytes(FULL_BLOCK_BYTES_HEX))
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
    BLOCK_TRANSACTIONS_COUNT = 11

    def setUp(self):
        self.btc_message_converter: AbstractBtcMessageConverter = None
        self.tx_service = None
        self.txns = []
        self.test_data = None
        self.short_ids = [i for i in range(1, 6)]
        self.magic = 12345
        self.version = 23456

    @multi_setup()
    def test_tx_msg_to_btc_tx_msg__success(self):
        tx_hash = Sha256Hash(helpers.generate_bytearray(SHA256_HASH_LEN))
        tx = mock_btc_messages.generate_btc_tx().payload()

        tx_msg = TxMessage(message_hash=tx_hash, network_num=12345, tx_val=tx)

        btc_tx_msg = self.btc_message_converter.bx_tx_to_tx(tx_msg)

        self.assertTrue(btc_tx_msg)
        self.assertEqual(btc_tx_msg.magic(), self.MAGIC)
        self.assertEqual(btc_tx_msg.command(), b"tx")
        self.assertEqual(btc_tx_msg.payload(), tx)

    @multi_setup()
    def test_tx_msg_to_btc_tx_msg__type_error(self):
        btc_tx_msg = mock_btc_messages.generate_btc_tx()

        self.assertRaises(TypeError, self.btc_message_converter.bx_tx_to_tx, btc_tx_msg)

    @multi_setup()
    def test_btc_tx_msg_to_tx_msg__success(self):
        btc_tx_msg = mock_btc_messages.generate_btc_tx()
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

        btc_block = BlockBtcMessage(
            self.magic, self.version, prev_block, merkle_root, timestamp, bits, nonce, self.txns
        )
        block_hash = btc_block.block_hash()

        bloxroute_block, block_info = self.btc_message_converter.block_to_bx_block(
            btc_block, self.tx_service, True, 0
        )
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
        self.assertEqual(prev_block_hash, parsed_btc_block.prev_block_hash().get_little_endian())
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
        bx_block, bx_block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
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
            bx_tx_hash = btc_common_utils.get_txid(txn)
            self.tx_service.assign_short_id(bx_tx_hash, short_id + 1)
            self.tx_service.set_transaction_contents(bx_tx_hash, txn)
        bx_block, block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
        ref_block, _, unknown_tx_sids, unknown_tx_hashes = self.btc_message_converter.bx_block_to_block(
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
            bx_tx_hash = btc_common_utils.get_txid(txn)
            self.tx_service.assign_short_id(bx_tx_hash, short_id + 1)
            self.tx_service.set_transaction_contents(bx_tx_hash, txn)
        bx_block, block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
        ref_block, ref_lock_info, unknown_tx_sids, unknown_tx_hashes = self.btc_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )
        self.assertEqual(len(block_info.short_ids), block_info.txn_count, "all txs were compressed")
        self.assertEqual(len(unknown_tx_hashes), 0)
        self.assertEqual(len(unknown_tx_sids), 0)
        self.assertEqual(
            parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes()
        )

    @multi_setup()
    def test_segwit_partial_compression(self):
        parsed_block = get_segwit_block()
        transactions_short = parsed_block.txns()[:]
        transactions_short = transactions_short[:int(len(transactions_short) * 0.9)]
        random.shuffle(transactions_short)

        for short_id, txn in enumerate(transactions_short):
            bx_tx_hash = btc_common_utils.get_txid(txn)
            self.tx_service.assign_short_id(bx_tx_hash, short_id + 1)
            self.tx_service.set_transaction_contents(bx_tx_hash, txn)
        bx_block, block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service, True, 0)
        self.assertEqual(len(transactions_short), len(block_info.short_ids), "not all txs were compressed")
        self.assertEqual(int(block_info.txn_count * 0.9), len(block_info.short_ids), "not all txs were compressed")
        ref_block, _, _, _ = self.btc_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )
        seg_bytes = parsed_block.rawbytes().tobytes()
        ref_bytes = ref_block.rawbytes().tobytes()

        self.assertEqual(
            seg_bytes, ref_bytes
        )

    @multi_setup()
    def test_compact_block_full_compression(self):
        compact_block = get_sample_compact_block()
        recovered_block = get_recovered_compact_block()
        for short_id, txn in enumerate(recovered_block.txns()):
            bx_tx_hash = BtcObjectHash(buf=crypto.double_sha256(txn),
                                       length=BTC_SHA_HASH_LEN)
            self.tx_service.set_transaction_contents(bx_tx_hash, txn)
            self.tx_service.assign_short_id(bx_tx_hash, short_id + 1)

        result = self.btc_message_converter.compact_block_to_bx_block(
            compact_block, self.tx_service
        )
        self.assertTrue(result.success)
        ref_block, _, _, _ = self.btc_message_converter.bx_block_to_block(
            result.bx_block, self.tx_service
        )
        self.assertEqual(recovered_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes())

    @multi_setup()
    def test_compact_block_partial_compression(self):
        compact_block = get_sample_compact_block()
        recovered_block = get_recovered_compact_block()
        recovered_transactions = []

        index = 0
        for idx, tx in enumerate(recovered_block.txns()):
            if index % 2 == 0:
                tx_hash = btc_common_utils.get_txid(tx)
                self.tx_service.assign_short_id(tx_hash, idx + 1)
                self.tx_service.set_transaction_contents(tx_hash, tx)
            else:
                recovered_transactions.append(tx)
            index += 1

        result = self.btc_message_converter.compact_block_to_bx_block(
            compact_block, self.tx_service
        )
        self.assertFalse(result.success)
        self.assertIsNone(result.bx_block)
        self.assertEqual(len(recovered_transactions), len(result.missing_indices))
        for missing_index in result.missing_indices:
            self.assertNotEqual(0, missing_index % 2)
        for recovered_transaction in recovered_transactions:
            result.recovered_transactions.append(recovered_transaction)
        result = self.btc_message_converter.recovered_compact_block_to_bx_block(result)
        self.assertTrue(result.success)
        ref_block, _, _, _ = self.btc_message_converter.bx_block_to_block(
            result.bx_block, self.tx_service
        )
        self.assertEqual(recovered_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes())

    def init(self, use_extensions: bool):
        opts = Namespace()
        opts.use_extensions = use_extensions
        opts.import_extensions = use_extensions
        opts.tx_mem_pool_bucket_size = DEFAULT_TX_MEM_POOL_BUCKET_SIZE
        btc_message_converter = converter_factory.create_btc_message_converter(self.MAGIC, opts=opts)
        if use_extensions:
            helpers.set_extensions_parallelism()
            tx_service = ExtensionTransactionService(MockNode(
                gateway_helpers.get_gateway_opts(8999)), 0)
        else:
            tx_service = TransactionService(MockNode(
                gateway_helpers.get_gateway_opts(8999)), 0)
        if self.txns:
            for idx, txn in enumerate(self.txns):
                sha = btc_common_utils.get_txid(txn)
                if idx % 2 == 0:
                    tx_service.assign_short_id(sha, self.short_ids[int(idx/2)])
                    tx_service.set_transaction_contents(sha, txn)
        return tx_service, btc_message_converter

    @multi_setup()
    def test_no_compression(self):
        parsed_block = get_sample_block()
        bx_block, block_info = self.btc_message_converter.block_to_bx_block(parsed_block, self.tx_service, False, 0)
        ref_block, _, unknown_tx_sids, unknown_tx_hashes = self.btc_message_converter.bx_block_to_block(
            bx_block, self.tx_service
        )

        self.assertTrue(len(bx_block) >= len(parsed_block.rawbytes()))
        self.assertEqual(0, len(block_info.short_ids))
        self.assertEqual(len(unknown_tx_hashes), 0)
        self.assertEqual(len(unknown_tx_sids), 0)
        self.assertEqual(
            parsed_block.rawbytes().tobytes(), ref_block.rawbytes().tobytes()
        )
