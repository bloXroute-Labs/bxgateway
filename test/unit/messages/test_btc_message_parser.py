from bxcommon.messages.btc.btc_message import BTCMessage
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.messages.ping_message import PingMessage
from bxcommon.messages.tx_message import TxMessage
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import generate_bytearray
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.messages.btc_message_parser import tx_msg_to_btc_tx_msg, btc_tx_msg_to_tx_msg


class BtcMessageParserTests(AbstractTestCase):

    def test_tx_msg_to_btc_tx_msg__success(self):
        msg_hash = ObjectHash(generate_bytearray(32))
        tx = generate_bytearray(250)
        magic = 123

        tx_msg = TxMessage(msg_hash=msg_hash,blob=tx)

        btc_tx_msg = tx_msg_to_btc_tx_msg(tx_msg, magic)

        self.assertTrue(btc_tx_msg)
        self.assertEqual(btc_tx_msg.magic(), magic)
        self.assertEqual(btc_tx_msg.command(), 'tx')
        self.assertEqual(btc_tx_msg.payload(), tx)

    def test_tx_msg_to_btc_tx_msg__type_error(self):
        btc_tx_msg = TxBTCMessage(buf=generate_bytearray(250))
        magic = 123

        self.assertRaises(TypeError, tx_msg_to_btc_tx_msg, btc_tx_msg, magic)

    def test_btc_tx_msg_to_tx_msg__success(self):
        btc_tx_msg = TxBTCMessage(buf=generate_bytearray(250))

        tx_msg = btc_tx_msg_to_tx_msg(btc_tx_msg)

        self.assertTrue(tx_msg)
        self.assertIsInstance(tx_msg, TxMessage)
        self.assertEqual(tx_msg.msg_hash(), btc_tx_msg.tx_hash())
        self.assertEqual(tx_msg.blob(), btc_tx_msg.tx())

    def test_btc_tx_msg_to_tx_msg__type_error(self):
        tx_msg = TxMessage(buf=generate_bytearray(250))

        self.assertRaises(TypeError, btc_tx_msg_to_tx_msg, tx_msg)
