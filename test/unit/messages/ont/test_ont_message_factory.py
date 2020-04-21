import time

from bxcommon.exceptions import PayloadLenError, ChecksumError
from bxcommon.test_utils.helpers import create_input_buffer_with_bytes
from bxcommon.test_utils.message_factory_test_case import MessageFactoryTestCase
from bxcommon.utils import crypto
from bxgateway.messages.ont.addr_ont_message import AddrOntMessage
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.consensus_ont_message import ConsensusOntMessage
from bxgateway.messages.ont.get_addr_ont_message import GetAddrOntMessage
from bxgateway.messages.ont.get_blocks_ont_message import GetBlocksOntMessage
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.get_headers_ont_message import GetHeadersOntMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage, InventoryOntType
from bxgateway.messages.ont.notfound_ont_message import NotFoundOntMessage
from bxgateway.messages.ont.ont_message_factory import ont_message_factory
from bxgateway.messages.ont.ping_ont_message import PingOntMessage
from bxgateway.messages.ont.pong_ont_message import PongOntMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.messages.ont.ver_ack_ont_message import VerAckOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage
from bxgateway.ont_constants import ONT_HEADER_MINUS_CHECKSUM, ONT_HDR_COMMON_OFF
from bxgateway.utils.ont.ont_object_hash import OntObjectHash


class OntMessageFactoryTest(MessageFactoryTestCase):
    MAGIC = 12345
    VERSION = 111
    HASH = OntObjectHash(binary=crypto.double_sha256(b"123"))

    VERSION_ONT_MESSAGE = VersionOntMessage(MAGIC, VERSION, 20330, 20330, 20330, bytes(32), 123, 0, True, True,
                                            "v1.0.0".encode("utf-8"))

    def get_message_factory(self):
        return ont_message_factory

    def test_peek_message_success_all_types(self):
        self.get_message_preview_successfully(self.VERSION_ONT_MESSAGE, VersionOntMessage.MESSAGE_TYPE, 83)
        self.get_message_preview_successfully(VerAckOntMessage(self.MAGIC, True), VerAckOntMessage.MESSAGE_TYPE, 1)
        self.get_message_preview_successfully(PingOntMessage(self.MAGIC), PingOntMessage.MESSAGE_TYPE, 8)
        self.get_message_preview_successfully(PongOntMessage(self.MAGIC, 123), PongOntMessage.MESSAGE_TYPE, 8)
        self.get_message_preview_successfully(GetAddrOntMessage(self.MAGIC), GetAddrOntMessage.MESSAGE_TYPE, 0)
        self.get_message_preview_successfully(AddrOntMessage(self.MAGIC, [(int(time.time()), 123, "127.0.0.1",
                                                                           20300, 20200, 1234)]),
                                              AddrOntMessage.MESSAGE_TYPE, 52)
        self.get_message_preview_successfully(ConsensusOntMessage(self.MAGIC, self.VERSION, bytes(20)),
                                              ConsensusOntMessage.MESSAGE_TYPE, 24)

        self.get_message_preview_successfully(
            InvOntMessage(self.MAGIC, InventoryOntType.MSG_TX, [self.HASH, self.HASH]),
            InvOntMessage.MESSAGE_TYPE, 69)
        self.get_message_preview_successfully(GetDataOntMessage(self.MAGIC, 1, self.HASH),
                                              GetDataOntMessage.MESSAGE_TYPE, 33)
        self.get_message_preview_successfully(GetHeadersOntMessage(self.MAGIC, 1, self.HASH, self.HASH),
                                              GetHeadersOntMessage.MESSAGE_TYPE, 65)
        self.get_message_preview_successfully(GetBlocksOntMessage(self.MAGIC, 1, self.HASH, self.HASH),
                                              GetBlocksOntMessage.MESSAGE_TYPE, 65)
        self.get_message_preview_successfully(TxOntMessage(self.MAGIC, self.VERSION, bytes(20)),
                                              TxOntMessage.MESSAGE_TYPE, 21)
        self.get_message_preview_successfully(BlockOntMessage(self.MAGIC, self.VERSION, self.HASH, self.HASH, self.HASH,
                                                              0, 0, 0, bytes(10), bytes(20), [bytes(33)] * 5,
                                                              [bytes(2)] * 3, [bytes(32)] * 5, self.HASH),
                                              BlockOntMessage.MESSAGE_TYPE, 524)
        self.get_message_preview_successfully(HeadersOntMessage(self.MAGIC, [bytes(1)] * 2),
                                              HeadersOntMessage.MESSAGE_TYPE, 6)
        self.get_message_preview_successfully(NotFoundOntMessage(self.MAGIC, self.HASH),
                                              NotFoundOntMessage.MESSAGE_TYPE, 32)

    def test_peek_message_incomplete(self):
        is_full_message, command, payload_length = ont_message_factory.get_message_header_preview_from_input_buffer(
            create_input_buffer_with_bytes(self.VERSION_ONT_MESSAGE.rawbytes()[:-10])
        )
        self.assertFalse(is_full_message)
        self.assertEqual(b"version", command)
        self.assertEqual(83, payload_length)

        is_full_message, command, payload_length = ont_message_factory.get_message_header_preview_from_input_buffer(
            create_input_buffer_with_bytes(self.VERSION_ONT_MESSAGE.rawbytes()[:1])
        )
        self.assertFalse(is_full_message)
        self.assertIsNone(command)
        self.assertIsNone(payload_length)

    def test_parse_message_success_all_types(self):
        self.create_message_successfully(self.VERSION_ONT_MESSAGE, VersionOntMessage)
        self.create_message_successfully(VerAckOntMessage(self.MAGIC, False), VerAckOntMessage)
        self.create_message_successfully(PingOntMessage(self.MAGIC), PingOntMessage)
        self.create_message_successfully(PongOntMessage(self.MAGIC, 123), PongOntMessage)
        self.create_message_successfully(GetAddrOntMessage(self.MAGIC), GetAddrOntMessage)
        self.create_message_successfully(AddrOntMessage(self.MAGIC, [(int(time.time()), 123, "127.0.0.1", 20300,
                                                                      20200, 1234)]), AddrOntMessage)
        self.create_message_successfully(ConsensusOntMessage(self.MAGIC, self.VERSION, bytes(20)),
                                         ConsensusOntMessage)
        self.create_message_successfully(InvOntMessage(self.MAGIC, InventoryOntType.MSG_TX, [self.HASH, self.HASH]),
                                         InvOntMessage)
        self.create_message_successfully(GetDataOntMessage(self.MAGIC, 1, self.HASH), GetDataOntMessage)
        self.create_message_successfully(GetHeadersOntMessage(self.MAGIC, 1, self.HASH, self.HASH),
                                         GetHeadersOntMessage)
        self.create_message_successfully(GetBlocksOntMessage(self.MAGIC, 1, self.HASH, self.HASH),
                                         GetBlocksOntMessage)
        self.create_message_successfully(TxOntMessage(self.MAGIC, self.VERSION, bytes(20)), TxOntMessage)
        self.create_message_successfully(BlockOntMessage(self.MAGIC, self.VERSION, self.HASH, self.HASH, self.HASH,
                                                         0, 0, 0, bytes(10), bytes(20), [bytes(33)] * 5,
                                                         [bytes(2)] * 3, [bytes(32)] * 5, self.HASH),
                                         BlockOntMessage)
        self.create_message_successfully(HeadersOntMessage(self.MAGIC, [bytes(1)] * 2), HeadersOntMessage)
        self.create_message_successfully(NotFoundOntMessage(self.MAGIC, self.HASH), NotFoundOntMessage)

    def test_parse_message_incomplete(self):
        with self.assertRaises(PayloadLenError):
            ont_message_factory.create_message_from_buffer(PingOntMessage(self.MAGIC).rawbytes()[:-1])

        ping_message = PingOntMessage(self.MAGIC)
        for i in range(ONT_HEADER_MINUS_CHECKSUM, ONT_HDR_COMMON_OFF):
            ping_message.buf[i] = 0
        with self.assertRaises(ChecksumError):
            ont_message_factory.create_message_from_buffer(ping_message.rawbytes())
