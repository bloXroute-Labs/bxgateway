import time

from bxcommon.test_utils.message_factory_test_case import MessageFactoryTestCase
from bxcommon.exceptions import PayloadLenError, ChecksumError
from bxcommon.test_utils import helpers
from bxcommon.test_utils.helpers import create_input_buffer_with_bytes
from bxcommon.utils import crypto, convert
from bxcommon.utils.blockchain_utils.btc import btc_common_utils
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
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

    def test_segwit_tx_hash(self):
        seg_tx = "010000000001024668fcfeba861f7f1bf4d386f15cc6923bd8425e0214686671775359d17a51d50100000000ffffffffdc5530f864de2fac86246426094d7b8586a452d6bd8c209bb891646afb3548770000000000ffffffff0220040000000000001600147771a1cab96e36344b1693d3d9f29180ca900482f5c40100000000001976a91483121cc1ea476c25d91191ff735a5e90518c732788ac02473044022006db5e6aa36dafb5d89a8522675d304a228d39ede1450aaab04f84b1fb57db2902203efb537cca9738c599d95d5c0ddcec6ebd11c6001541a89a468246318e0bd6fe012102d6b8b2ba44eb621ac9537ed7e11553bb02060abca88a9e6faf7697df5ac6d30c02483045022100b04869e06930db5d4e8e4d453d9aed1097a8dae57eef0274ebdc99a106796335022037a6b744900b9b6392448c961e8d793367a0caf675b9ca80349c593e505d8e9d0121034ef9635ae7cd714b2cf8af7e72f23b8b07c7f75d75df95da8d682ae17459091b00000000"
        seg_tx_bytes = convert.hex_to_bytes(seg_tx)
        self.assertTrue(btc_common_utils.is_segwit(seg_tx_bytes))
        self.assertEqual(convert.bytes_to_hex(btc_common_utils.get_txid(seg_tx_bytes).binary),
                         "d9a057f11a21cf8afd32278e23fd2290660f05a3ffb582466eb5a1a5ece4ce85")

    def test_non_segwit_tx_hash(self):
        non_seg_tx = "0100000002d8c8df6a6fdd2addaf589a83d860f18b44872d13ee6ec3526b2b470d42a96d4d000000008b483045022100b31557e47191936cb14e013fb421b1860b5e4fd5d2bc5ec1938f4ffb1651dc8902202661c2920771fd29dd91cd4100cefb971269836da4914d970d333861819265ba014104c54f8ea9507f31a05ae325616e3024bd9878cb0a5dff780444002d731577be4e2e69c663ff2da922902a4454841aa1754c1b6292ad7d317150308d8cce0ad7abffffffff2ab3fa4f68a512266134085d3260b94d3b6cfd351450cff021c045a69ba120b2000000008b4830450220230110bc99ef311f1f8bda9d0d968bfe5dfa4af171adbef9ef71678d658823bf022100f956d4fcfa0995a578d84e7e913f9bb1cf5b5be1440bcede07bce9cd5b38115d014104c6ec27cffce0823c3fecb162dbd576c88dd7cda0b7b32b0961188a392b488c94ca174d833ee6a9b71c0996620ae71e799fc7c77901db147fa7d97732e49c8226ffffffff02c0175302000000001976a914a3d89c53bb956f08917b44d113c6b2bcbe0c29b788acc01c3d09000000001976a91408338e1d5e26db3fce21b011795b1c3c8a5a5d0788ac00000000"
        non_seg_tx_bytes = convert.hex_to_bytes(non_seg_tx)
        self.assertFalse(btc_common_utils.is_segwit(non_seg_tx_bytes))
        self.assertEqual(convert.bytes_to_hex(btc_common_utils.get_txid(non_seg_tx_bytes).binary),
                         "9021b49d445c719106c95d561b9c3fac7bcb3650db67684a9226cd7fa1e1c1a0")