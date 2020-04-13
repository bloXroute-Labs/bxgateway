import struct

from bxcommon.constants import MSG_NULL_BYTE
from bxcommon.exceptions import ChecksumError, PayloadLenError
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import crypto
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_HEADER_MINUS_CHECKSUM, BTC_MAGIC_NUMBERS
from bxutils import logging
from bxgateway import log_messages

logger = logging.get_logger(__name__)


class BtcMessage(AbstractMessage):
    MESSAGE_TYPE = b"btc"
    HEADER_LENGTH = BTC_HDR_COMMON_OFF

    def __init__(self, magic=None, command=None, payload_len=None, buf=None):
        self.buf = buf
        self._memoryview = memoryview(buf)

        magic_num = magic if magic not in BTC_MAGIC_NUMBERS else BTC_MAGIC_NUMBERS[magic]
        checksum = crypto.bitcoin_hash(self._memoryview[BTC_HDR_COMMON_OFF:payload_len + BTC_HDR_COMMON_OFF])

        off = 0
        struct.pack_into("<L12sL", buf, off, magic_num, command, payload_len)
        off += 20
        buf[off:off + 4] = checksum[0:4]

        self._magic = magic_num
        self._command = command
        self._payload_len = payload_len
        self._payload = None
        self._checksum = None

    # TODO: pull this out in to a message protocol
    @classmethod
    def unpack(cls, buf):
        magic, command, payload_length = struct.unpack_from("<L12sL", buf)
        checksum = buf[BTC_HEADER_MINUS_CHECKSUM:cls.HEADER_LENGTH]
        return command.rstrip(MSG_NULL_BYTE), magic, checksum, payload_length

    @classmethod
    def validate_payload(cls, buf, unpacked_args):
        command, _magic, checksum, payload_length = unpacked_args
        if payload_length != len(buf) - cls.HEADER_LENGTH:
            error_message = log_messages.PAYLOAD_LENGTH_MISMATCH.text.format(payload_length, len(buf))
            logger.error(error_message)
            raise PayloadLenError(error_message)
        ref_checksum = crypto.bitcoin_hash(buf[cls.HEADER_LENGTH:cls.HEADER_LENGTH + payload_length])[0:4]
        if checksum != ref_checksum:
            error_message = log_messages.PACKET_CHECKSUM_MISMATCH.text.format(checksum, ref_checksum, repr(buf))
            logger.error(error_message)
            raise ChecksumError(error_message, buf)

    @classmethod
    def initialize_class(cls, cls_type, buf, unpacked_args):
        instance = cls_type(buf=buf)
        return instance

    # END TODO

    def rawbytes(self) -> memoryview:
        """
        Returns a memoryview of the message
        """
        if self._payload_len is None:
            self._payload_len = struct.unpack_from("<L", self.buf, 16)[0]

        if self._payload_len + BTC_HDR_COMMON_OFF == len(self.buf):
            return self._memoryview
        else:
            return self._memoryview[0:self._payload_len + BTC_HDR_COMMON_OFF]

    def magic(self):
        if self._magic is None:
            self._magic, = struct.unpack_from("<L", self.buf)
        return self._magic

    def command(self):
        if self._command is None:
            self._command = struct.unpack_from("<12s", self.buf, 4)[0].rstrip(MSG_NULL_BYTE)
        return self._command

    def payload_len(self):
        if self._payload_len is None:
            self._payload_len, = struct.unpack_from("<L", self.buf, 16)
        return self._payload_len

    def checksum(self):
        if self._checksum is None:
            self._checksum = self.buf[BTC_HEADER_MINUS_CHECKSUM:BTC_HDR_COMMON_OFF]
        return self._checksum

    def payload(self):
        if self._payload is None:
            self._payload = self.buf[BTC_HDR_COMMON_OFF:self.payload_len() + BTC_HDR_COMMON_OFF]
        return self._payload

    def __eq__(self, other):
        """
        Expensive equality comparison. Use only for tests.
        """
        if not isinstance(other, BtcMessage):
            return False
        else:
            return self.rawbytes().tobytes() == other.rawbytes().tobytes()

    def __repr__(self):
        return "BtcMessage<command: {}, length: {}>".format(self.MESSAGE_TYPE, len(self.rawbytes()))
