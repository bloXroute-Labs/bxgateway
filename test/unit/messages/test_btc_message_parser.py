from bxcommon import constants
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.messages.tx_message import TxMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.messages.btc_message_parser import btc_tx_msg_to_tx_msg, tx_msg_to_btc_tx_msg


class BtcMessageParserTests(AbstractTestCase):
    average_tx_size = 250

    def test_tx_msg_to_btc_tx_msg__success(self):
        tx_hash = ObjectHash(helpers.generate_bytearray(constants.SHA256_HASH_LEN))
        tx = helpers.generate_bytearray(self.average_tx_size)
        magic = 123

        tx_msg = TxMessage(tx_hash=tx_hash, tx_val=tx)

        btc_tx_msg = tx_msg_to_btc_tx_msg(tx_msg, magic)

        self.assertTrue(btc_tx_msg)
        self.assertEqual(btc_tx_msg.magic(), magic)
        self.assertEqual(btc_tx_msg.command(), 'tx')
        self.assertEqual(btc_tx_msg.payload(), tx)

    def test_tx_msg_to_btc_tx_msg__type_error(self):
        btc_tx_msg = TxBTCMessage(buf=helpers.generate_bytearray(self.average_tx_size))
        magic = 123

        self.assertRaises(TypeError, tx_msg_to_btc_tx_msg, btc_tx_msg, magic)

    def test_btc_tx_msg_to_tx_msg__success(self):
        btc_tx_msg = TxBTCMessage(buf=helpers.generate_bytearray(self.average_tx_size))

        tx_msg = btc_tx_msg_to_tx_msg(btc_tx_msg)

        self.assertTrue(tx_msg)
        self.assertIsInstance(tx_msg, TxMessage)
        self.assertEqual(tx_msg.tx_hash(), btc_tx_msg.tx_hash())
        self.assertEqual(tx_msg.tx_val(), btc_tx_msg.tx())

    def test_btc_tx_msg_to_tx_msg__type_error(self):
        tx_msg = TxMessage(buf=helpers.generate_bytearray(self.average_tx_size))

        self.assertRaises(TypeError, btc_tx_msg_to_tx_msg, tx_msg)
