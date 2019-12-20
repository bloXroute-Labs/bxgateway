import struct
from typing import Optional

from bxutils import logging
from bxutils.logging.log_level import LogLevel

from bxcommon.constants import MSG_NULL_BYTE
from bxcommon.exceptions import ChecksumError, PayloadLenError
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import crypto
from bxgateway import ont_constants

logger = logging.get_logger(__name__)


class OntMessage(AbstractMessage):
    MESSAGE_TYPE = b"ont"
    HEADER_LENGTH = ont_constants.ONT_HDR_COMMON_OFF

    def __init__(self, magic: Optional[int] = None, command: Optional[bytes] = None, payload_len: Optional[int] = None,
                 buf: Optional[bytearray] = None):
        self.buf = buf
        self._memoryview = memoryview(buf)

        magic_num = magic if magic not in ont_constants.ONT_MAGIC_NUMBERS else ont_constants.ONT_MAGIC_NUMBERS[magic]
        checksum = crypto.double_sha256(self._memoryview[ont_constants.ONT_HDR_COMMON_OFF:payload_len +
                                        ont_constants.ONT_HDR_COMMON_OFF])

        off = 0
        struct.pack_into("<L12sL", buf, off, magic_num, command, payload_len)
        off += ont_constants.ONT_HEADER_MINUS_CHECKSUM
        buf[off:off + ont_constants.ONT_MSG_CHECKSUM_LEN] = checksum[0:4]

        self._magic = magic_num
        self._command = command
        self._payload_len = payload_len
        self._payload = None
        self._checksum = None

    def __repr__(self):
        return "OntMessage<command: {}, length: {}>".format(self.MESSAGE_TYPE, len(self.rawbytes()))

    @classmethod
    def unpack(cls, buf):
        magic, command, payload_length = struct.unpack_from("<L12sL", buf)
        checksum = buf[ont_constants.ONT_HEADER_MINUS_CHECKSUM:cls.HEADER_LENGTH]
        return command.rstrip(MSG_NULL_BYTE), magic, checksum, payload_length

    @classmethod
    def validate_payload(cls, buf, unpacked_args):
        command, _magic, checksum, payload_length = unpacked_args
        if payload_length != len(buf) - cls.HEADER_LENGTH:
            logger.error("Payload length does not match buffer size: {} vs {} bytes", payload_length, len(buf))
            raise PayloadLenError("Payload length error raised")
        ref_checksum = crypto.double_sha256(buf[cls.HEADER_LENGTH:cls.HEADER_LENGTH + payload_length])[0:4]
        if checksum != ref_checksum:
            logger.error("Checksum ({}) for packet doesn't match ({}): {}", checksum, ref_checksum, buf)
            raise ChecksumError("Checksum error raised", buf)

    @classmethod
    def initialize_class(cls, cls_type, buf, unpacked_args):
        return cls_type(buf=buf)

    def rawbytes(self) -> memoryview:
        assert self.buf is not None
        if self._payload_len is None:
            self._payload_len, = struct.unpack_from("<L", self.buf, 16)
        assert self._payload_len is not None
        if self._payload_len + ont_constants.ONT_HDR_COMMON_OFF == len(self.buf):
            return self._memoryview
        else:
            return self._memoryview[0:self._payload_len + ont_constants.ONT_HDR_COMMON_OFF]

    def magic(self) -> int:
        if self._magic is None:
            assert self.buf is not None
            self._magic, = struct.unpack_from("<L", self.buf)
        return self._magic

    def command(self) -> bytes:
        if self._command is None:
            assert self.buf is not None
            self._command = struct.unpack_from("<12s", self.buf, 4)[0].rstrip(MSG_NULL_BYTE)
        assert self._payload_len is not None
        return self._command

    def payload_len(self) -> int:
        if self._payload_len is None:
            assert self.buf is not None
            self._payload_len, = struct.unpack_from("<L", self.buf, 16)
        assert self._payload_len is not None
        return self._payload_len

    def payload(self) -> bytearray:
        if self._payload is None:
            assert self.buf is not None
            self._payload = self.buf[ont_constants.ONT_HDR_COMMON_OFF:self.payload_len() +
                                     ont_constants.ONT_HDR_COMMON_OFF]
        return self._payload
