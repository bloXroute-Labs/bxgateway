import time

from bxcommon.test_utils.message_factory_test_case import MessageFactoryTestCase
from bxcommon.exceptions import PayloadLenError, ChecksumError
from bxcommon.test_utils import helpers
from bxcommon.test_utils.helpers import create_input_buffer_with_bytes
from bxcommon.utils import crypto
from bxgateway.btc_constants import BTC_HEADER_MINUS_CHECKSUM, BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.addr_btc_message import AddrBtcMessage
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_factory import btc_message_factory
from bxgateway.messages.btc.data_btc_message import GetHeadersBtcMessage, GetBlocksBtcMessage
from bxgateway.messages.btc.get_addr_btc_message import GetAddrBtcMessage
from bxgateway.messages.btc.headers_btc_message import HeadersBtcMessage
from bxgateway.messages.btc.inventory_btc_message import InvBtcMessage, GetDataBtcMessage, NotFoundBtcMessage
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage
from bxgateway.messages.btc.pong_btc_message import PongBtcMessage
from bxgateway.messages.btc.reject_btc_message import RejectBtcMessage
from bxgateway.messages.btc.send_headers_btc_message import SendHeadersBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxIn, TxBtcMessage
from bxgateway.messages.btc.ver_ack_btc_message import VerAckBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage
from bxgateway.messages.btc.fee_filter_btc_message import FeeFilterBtcMessage
from bxgateway.messages.btc.xversion_btc_message import XversionBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcMessageFactoryTest(MessageFactoryTestCase):
    MAGIC = 12345
    VERSION = 11111
    HASH = BtcObjectHash(binary=crypto.bitcoin_hash(b"123"))

    VERSION_BTC_MESSAGE = VersionBtcMessage(MAGIC, VERSION, "127.0.0.1", 8000, "127.0.0.1", 8001, 123, 0,
                                            "hello".encode("utf-8"))

    def get_message_factory(self):
        return btc_message_factory

    def test_peek_message_success_all_types(self):
        # TODO: pull these numbers into constants, along with all the BTC messages
        self.get_message_preview_successfully(self.VERSION_BTC_MESSAGE, VersionBtcMessage.MESSAGE_TYPE, 90)
        self.get_message_preview_successfully(VerAckBtcMessage(self.MAGIC), VerAckBtcMessage.MESSAGE_TYPE, 0)
        self.get_message_preview_successfully(PingBtcMessage(self.MAGIC), PingBtcMessage.MESSAGE_TYPE, 8)
        self.get_message_preview_successfully(PongBtcMessage(self.MAGIC, 123), PongBtcMessage.MESSAGE_TYPE, 8)
        self.get_message_preview_successfully(GetAddrBtcMessage(self.MAGIC), GetAddrBtcMessage.MESSAGE_TYPE, 0)
        self.get_message_preview_successfully(AddrBtcMessage(self.MAGIC, [(int(time.time()), "127.0.0.1", 8000)]),
                                              AddrBtcMessage.MESSAGE_TYPE, 23)

        inv_vector = [(1, self.HASH), (2, self.HASH)]
        self.get_message_preview_successfully(InvBtcMessage(self.MAGIC, inv_vector), InvBtcMessage.MESSAGE_TYPE, 73)
        self.get_message_preview_successfully(GetDataBtcMessage(self.MAGIC, inv_vector), GetDataBtcMessage.MESSAGE_TYPE, 73)
        self.get_message_preview_successfully(NotFoundBtcMessage(self.MAGIC, inv_vector), NotFoundBtcMessage.MESSAGE_TYPE, 73)

        hashes = [self.HASH, self.HASH]
        self.get_message_preview_successfully(GetHeadersBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                              GetHeadersBtcMessage.MESSAGE_TYPE, 101)
        self.get_message_preview_successfully(GetBlocksBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                              GetBlocksBtcMessage.MESSAGE_TYPE, 101)

        self.get_message_preview_successfully(TxBtcMessage(self.MAGIC, self.VERSION, [], [], 0), TxBtcMessage.MESSAGE_TYPE, 10)

        txs = [TxIn(buf=bytearray(10), length=10, off=0).rawbytes()] * 5
        self.get_message_preview_successfully(BlockBtcMessage(self.MAGIC, self.VERSION, self.HASH, self.HASH, 0, 0, 0, txs),
                                              BlockBtcMessage.MESSAGE_TYPE, 131)
        self.get_message_preview_successfully(HeadersBtcMessage(self.MAGIC, [helpers.generate_bytearray(81)] * 2),
                                              HeadersBtcMessage.MESSAGE_TYPE, 163)
        self.get_message_preview_successfully(RejectBtcMessage(self.MAGIC, b"a message", RejectBtcMessage.REJECT_MALFORMED,
                                                        b"test break", helpers.generate_bytearray(10)),
                                              RejectBtcMessage.MESSAGE_TYPE, 32)
        self.get_message_preview_successfully(SendHeadersBtcMessage(self.MAGIC), SendHeadersBtcMessage.MESSAGE_TYPE, 0)
        self.get_message_preview_successfully(FeeFilterBtcMessage(self.MAGIC, fee_rate=100), FeeFilterBtcMessage.MESSAGE_TYPE, 8)
        self.get_message_preview_successfully(BtcMessage(self.MAGIC, b'xversion', 0, bytearray(30)), XversionBtcMessage.MESSAGE_TYPE, 0)

    def test_peek_message_incomplete(self):
        is_full_message, command, payload_length = btc_message_factory.get_message_header_preview_from_input_buffer(
            create_input_buffer_with_bytes(self.VERSION_BTC_MESSAGE.rawbytes()[:-10])
        )
        self.assertFalse(is_full_message)
        self.assertEqual(b"version", command)
        self.assertEqual(90, payload_length)

        is_full_message, command, payload_length = btc_message_factory.get_message_header_preview_from_input_buffer(
            create_input_buffer_with_bytes(self.VERSION_BTC_MESSAGE.rawbytes()[:1])
        )
        self.assertFalse(is_full_message)
        self.assertIsNone(command)
        self.assertIsNone(payload_length)

    def test_parse_message_success_all_types(self):
        # TODO: pull these numbers into constants, along with all the BTC messages
        self.create_message_successfully(self.VERSION_BTC_MESSAGE, VersionBtcMessage)
        self.create_message_successfully(VerAckBtcMessage(self.MAGIC), VerAckBtcMessage)
        self.create_message_successfully(PingBtcMessage(self.MAGIC), PingBtcMessage)
        self.create_message_successfully(PongBtcMessage(self.MAGIC, 123), PongBtcMessage)
        self.create_message_successfully(GetAddrBtcMessage(self.MAGIC), GetAddrBtcMessage)
        self.create_message_successfully(AddrBtcMessage(self.MAGIC, [(int(time.time()), "127.0.0.1", 8000)]), AddrBtcMessage)

        inv_vector = [(1, self.HASH), (2, self.HASH)]
        self.create_message_successfully(InvBtcMessage(self.MAGIC, inv_vector), InvBtcMessage)
        self.create_message_successfully(GetDataBtcMessage(self.MAGIC, inv_vector), GetDataBtcMessage)
        self.create_message_successfully(NotFoundBtcMessage(self.MAGIC, inv_vector), NotFoundBtcMessage)

        hashes = [self.HASH, self.HASH]
        self.create_message_successfully(GetHeadersBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                         GetHeadersBtcMessage)
        self.create_message_successfully(GetBlocksBtcMessage(self.MAGIC, self.VERSION, hashes, self.HASH),
                                         GetBlocksBtcMessage)

        self.create_message_successfully(TxBtcMessage(self.MAGIC, self.VERSION, [], [], 0), TxBtcMessage)

        txs = [TxIn(buf=bytearray(10), length=10, off=0).rawbytes()] * 5
        self.create_message_successfully(BlockBtcMessage(self.MAGIC, self.VERSION, self.HASH, self.HASH, 0, 0, 0, txs),
                                         BlockBtcMessage)
        self.create_message_successfully(HeadersBtcMessage(self.MAGIC, [helpers.generate_bytearray(81)] * 2),
                                         HeadersBtcMessage)
        self.create_message_successfully(RejectBtcMessage(self.MAGIC, b"a message", RejectBtcMessage.REJECT_MALFORMED,
                                                         b"test break", helpers.generate_bytearray(10)),
                                         RejectBtcMessage)
        self.create_message_successfully(SendHeadersBtcMessage(self.MAGIC), SendHeadersBtcMessage)

        self.create_message_successfully(FeeFilterBtcMessage(self.MAGIC, fee_rate=100), FeeFilterBtcMessage)

        self.create_message_successfully(BtcMessage(self.MAGIC, b'xversion', 0, bytearray(30)), XversionBtcMessage)

    def test_parse_message_incomplete(self):
        with self.assertRaises(PayloadLenError):
            btc_message_factory.create_message_from_buffer(PingBtcMessage(self.MAGIC).rawbytes()[:-1])

        ping_message = PingBtcMessage(self.MAGIC)
        for i in range(BTC_HEADER_MINUS_CHECKSUM, BTC_HDR_COMMON_OFF):
            ping_message.buf[i] = 0
        with self.assertRaises(ChecksumError):
            btc_message_factory.create_message_from_buffer(ping_message.rawbytes())
