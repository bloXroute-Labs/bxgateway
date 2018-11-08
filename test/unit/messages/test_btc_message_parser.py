from mock import MagicMock

from bxcommon.constants import BTC_HDR_COMMON_OFF, NULL_TX_SID
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.btc.block_btc_message import BlockBTCMessage
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import crypto
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import BTCObjectHash, ObjectHash
from bxgateway.messages import btc_message_parser


class BtcMessageParserTests(AbstractTestCase):
    AVERAGE_TX_SIZE = 250

    def test_tx_msg_to_btc_tx_msg__success(self):
        tx_hash = ObjectHash(helpers.generate_bytearray(SHA256_HASH_LEN))
        tx = helpers.generate_bytearray(self.AVERAGE_TX_SIZE)
        magic = 123

        tx_msg = TxMessage(tx_hash=tx_hash, tx_val=tx)

        btc_tx_msg = btc_message_parser.tx_msg_to_btc_tx_msg(tx_msg, magic)

        self.assertTrue(btc_tx_msg)
        self.assertEqual(btc_tx_msg.magic(), magic)
        self.assertEqual(btc_tx_msg.command(), 'tx')
        self.assertEqual(btc_tx_msg.payload(), tx)

    def test_tx_msg_to_btc_tx_msg__type_error(self):
        btc_tx_msg = TxBTCMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))
        magic = 123

        self.assertRaises(TypeError, btc_message_parser.tx_msg_to_btc_tx_msg, btc_tx_msg, magic)

    def test_btc_tx_msg_to_tx_msg__success(self):
        btc_tx_msg = TxBTCMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))

        tx_msg = btc_message_parser.btc_tx_msg_to_tx_msg(btc_tx_msg)

        self.assertTrue(tx_msg)
        self.assertIsInstance(tx_msg, TxMessage)
        self.assertEqual(tx_msg.tx_hash(), btc_tx_msg.tx_hash())
        self.assertEqual(tx_msg.tx_val(), btc_tx_msg.tx())

    def test_btc_tx_msg_to_tx_msg__type_error(self):
        tx_msg = TxMessage(buf=helpers.generate_bytearray(self.AVERAGE_TX_SIZE))

        self.assertRaises(TypeError, btc_message_parser.btc_tx_msg_to_tx_msg, tx_msg)

    def test_btc_block_to_bloxroute_block_and_back_sids_found(self):
        magic = 12345
        version = 23456
        prev_block_hash = bytearray(crypto.bitcoin_hash("123"))
        prev_block = BTCObjectHash(prev_block_hash, length=SHA256_HASH_LEN)
        merkle_root_hash = bytearray(crypto.bitcoin_hash("234"))
        merkle_root = BTCObjectHash(merkle_root_hash, length=SHA256_HASH_LEN)
        timestamp = 1
        bits = 2
        nonce = 3

        txns = [TxBTCMessage(magic, version, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]
        txn_hashes = map(lambda x: BTCObjectHash(buf=crypto.bitcoin_hash(x), length=SHA256_HASH_LEN), txns)
        short_ids = [i for i in xrange(5)]

        btc_block = BlockBTCMessage(magic, version, prev_block, merkle_root, timestamp, bits, nonce, txns)

        # find an sid for half the transactions
        def get_txid(txhash):
            index = txn_hashes.index(txhash)
            if index % 2 == 0:
                return short_ids[int(index / 2)]
            else:
                return NULL_TX_SID

        # return a transaction's info for assigned sids
        def get_tx_from_sid(sid):
            return txn_hashes[sid * 2], txns[sid * 2]

        tx_service = MagicMock()
        tx_service.get_txid = get_txid
        tx_service.get_tx_from_sid = get_tx_from_sid

        bloxroute_block = btc_message_parser.btc_block_to_bloxroute_block(btc_block, tx_service)
        # TODO: if we convert bloxroute block to a class, add some tests here

        parsed_btc_block, block_hash, _, _ = btc_message_parser.bloxroute_block_to_btc_block(bloxroute_block,
                                                                                             tx_service)
        self.assertIsNotNone(parsed_btc_block)
        self.assertEqual(version, parsed_btc_block.version())
        self.assertEqual(magic, parsed_btc_block.magic())
        self.assertEqual(prev_block_hash, parsed_btc_block.prev_block().get_little_endian())
        self.assertEqual(merkle_root_hash, parsed_btc_block.merkle_root().get_little_endian())
        self.assertEqual(timestamp, parsed_btc_block.timestamp())
        self.assertEqual(bits, parsed_btc_block.bits())
        self.assertEqual(nonce, parsed_btc_block.nonce())
        self.assertEqual(len(txns), parsed_btc_block.txn_count())
