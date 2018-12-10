import time

from bxcommon.exceptions import PayloadLenError, ChecksumError
from bxgateway.btc_constants import BTC_HEADER_MINUS_CHECKSUM, BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.addr_btc_message import AddrBtcMessage
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message_factory import btc_message_factory
from bxgateway.messages.btc.data_btc_message import GetHeadersBtcMessage, GetBlocksBtcMessage
from bxgateway.messages.btc.get_addr_btc_message import GetAddrBtcMessage
from bxgateway.messages.btc.header_btc_message import HeadersBtcMessage
from bxgateway.messages.btc.inventory_btc_message import InvBtcMessage, GetDataBtcMessage, NotFoundBtcMessage
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage
from bxgateway.messages.btc.pong_btc_message import PongBtcMessage
from bxgateway.messages.btc.reject_btc_message import RejectBtcMessage
from bxgateway.messages.btc.send_headers_btc_message import SendHeadersBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxIn, TxBtcMessage
from bxgateway.messages.btc.ver_ack_btc_message import VerAckBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import create_input_buffer_with_bytes, create_input_buffer_with_message
from bxcommon.utils import crypto
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcMessageFactoryTest(AbstractTestCase):
    MAGIC = 12345
    VERSION = 11111
    HASH = BtcObjectHash(binary=crypto.bitcoin_hash("123"))

    VERSION_BTC_MESSAGE = VersionBtcMessage(MAGIC, VERSION, "127.0.0.1", 8000, "127.0.0.1", 8001, 123, 0, "hello")

    def peek_message_successfully(self, message, expected_command, expected_payload_length):
        is_full_message, command, payload_length = btc_message_factory.get_message_header_preview(
            create_input_buffer_with_message(message)
        )
        self.assertTrue(is_full_message)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_payload_length, payload_length)

    def parse_message_successfully(self, message, message_type):
        result = btc_message_factory.create_message_from_buffer(message.rawbytes())
        self.assertIsInstance(result, message_type)
        return result

    def test_peek_message_success_all_types(self):
        # TODO: pull these numbers into constants, along with all the BTC messages
        self.peek_message_successfully(self.VERSION_BTC_MESSAGE, VersionBtcMessage.MESSAGE_TYPE, 90)
        self.peek_message_successfully(VerAckBtcMessage(self.MAGIC), VerAckBtcMessage.MESSAGE_TYPE, 0)
        self.peek_message_successfully(PingBtcMessage(self.MAGIC), PingBtcMessage.MESSAGE_TYPE, 8)
        self.peek_message_successfully(PongBtcMessage(self.MAGIC, 123), PongBtcMessage.MESSAGE_TYPE, 8)
        self.peek_message_successfully(GetAddrBtcMessage(self.MAGIC), GetAddrBtcMessage.MESSAGE_TYPE, 0)
        self.peek_message_successfully(AddrBtcMessage(self.MAGIC, [(time.time(), "127.0.0.1", 8000)]),
                                       AddrBtcMessage.MESSAGE_TYPE, 23)

        inv_vector = [(1, self.HASH), (2, self.HASH)]
        self.peek_message_successfully(InvBtcMessage(self.MAGIC, inv_vector), InvBtcMessage.MESSAGE_TYPE, 73)
        self.peek_message_successfully(GetDataBtcMessage(self.MAGIC, inv_vector), GetDataBtcMessage.MESSAGE_TYPE, 73)
        self.peek_message_successfully(NotFoundBtcMessage(self.MAGIC, inv_vector), NotFoundBtcMessage.MESSAGE_TYPE, 73)

        hashes = [self.HASH, self.HASH]
        self.peek_message_successfully(GetHeadersBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                       GetHeadersBtcMessage.MESSAGE_TYPE, 101)
        self.peek_message_successfully(GetBlocksBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                       GetBlocksBtcMessage.MESSAGE_TYPE, 101)

        self.peek_message_successfully(TxBtcMessage(self.MAGIC, self.VERSION, [], [], 0), TxBtcMessage.MESSAGE_TYPE, 10)

        txs = [TxIn(buf=bytearray(10), length=10, off=0).rawbytes()] * 5
        self.peek_message_successfully(BlockBtcMessage(self.MAGIC, self.VERSION, self.HASH, self.HASH, 0, 0, 0, txs),
                                       BlockBtcMessage.MESSAGE_TYPE, 131)
        self.peek_message_successfully(HeadersBtcMessage(self.MAGIC, [helpers.generate_bytearray(81)] * 2),
                                       HeadersBtcMessage.MESSAGE_TYPE, 163)
        self.peek_message_successfully(RejectBtcMessage(self.MAGIC, "a message", RejectBtcMessage.REJECT_MALFORMED,
                                                        "test break", helpers.generate_bytearray(10)),
                                       RejectBtcMessage.MESSAGE_TYPE, 32)
        self.peek_message_successfully(SendHeadersBtcMessage(self.MAGIC), SendHeadersBtcMessage.MESSAGE_TYPE, 0)

    def test_peek_message_incomplete(self):
        is_full_message, command, payload_length = btc_message_factory.get_message_header_preview(
            create_input_buffer_with_bytes(self.VERSION_BTC_MESSAGE.rawbytes()[:-10])
        )
        self.assertFalse(is_full_message)
        self.assertEquals("version", command)
        self.assertEquals(90, payload_length)

        is_full_message, command, payload_length = btc_message_factory.get_message_header_preview(
            create_input_buffer_with_bytes(self.VERSION_BTC_MESSAGE.rawbytes()[:1])
        )
        self.assertFalse(is_full_message)
        self.assertIsNone(command)
        self.assertIsNone(payload_length)

    def test_parse_message_success_all_types(self):
        # TODO: pull these numbers into constants, along with all the BTC messages
        self.parse_message_successfully(self.VERSION_BTC_MESSAGE, VersionBtcMessage)
        self.parse_message_successfully(VerAckBtcMessage(self.MAGIC), VerAckBtcMessage)
        self.parse_message_successfully(PingBtcMessage(self.MAGIC), PingBtcMessage)
        self.parse_message_successfully(PongBtcMessage(self.MAGIC, 123), PongBtcMessage)
        self.parse_message_successfully(GetAddrBtcMessage(self.MAGIC), GetAddrBtcMessage)
        self.parse_message_successfully(AddrBtcMessage(self.MAGIC, [(time.time(), "127.0.0.1", 8000)]), AddrBtcMessage)

        inv_vector = [(1, self.HASH), (2, self.HASH)]
        self.parse_message_successfully(InvBtcMessage(self.MAGIC, inv_vector), InvBtcMessage)
        self.parse_message_successfully(GetDataBtcMessage(self.MAGIC, inv_vector), GetDataBtcMessage)
        self.parse_message_successfully(NotFoundBtcMessage(self.MAGIC, inv_vector), NotFoundBtcMessage)

        hashes = [self.HASH, self.HASH]
        self.parse_message_successfully(GetHeadersBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                        GetHeadersBtcMessage)
        self.parse_message_successfully(GetBlocksBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                        GetBlocksBtcMessage)

        self.parse_message_successfully(TxBtcMessage(self.MAGIC, self.VERSION, [], [], 0), TxBtcMessage)

        txs = [TxIn(buf=bytearray(10), length=10, off=0).rawbytes()] * 5
        self.parse_message_successfully(BlockBtcMessage(self.MAGIC, self.VERSION, self.HASH, self.HASH, 0, 0, 0, txs),
                                        BlockBtcMessage)
        self.parse_message_successfully(HeadersBtcMessage(self.MAGIC, [helpers.generate_bytearray(81)] * 2),
                                        HeadersBtcMessage)
        self.parse_message_successfully(RejectBtcMessage(self.MAGIC, "a message", RejectBtcMessage.REJECT_MALFORMED,
                                                         "test break", helpers.generate_bytearray(10)),
                                        RejectBtcMessage)
        self.parse_message_successfully(SendHeadersBtcMessage(self.MAGIC), SendHeadersBtcMessage)

    def test_parse_message_incomplete(self):
        with self.assertRaises(PayloadLenError):
            btc_message_factory.create_message_from_buffer(PingBtcMessage(self.MAGIC).rawbytes()[:-1])

        ping_message = PingBtcMessage(self.MAGIC)
        for i in xrange(BTC_HEADER_MINUS_CHECKSUM, BTC_HDR_COMMON_OFF):
            ping_message.buf[i] = 0
        with self.assertRaises(ChecksumError):
            btc_message_factory.create_message_from_buffer(ping_message.rawbytes())
