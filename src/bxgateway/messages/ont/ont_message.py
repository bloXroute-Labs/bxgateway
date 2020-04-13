import struct
from typing import Optional

from bxcommon.constants import MSG_NULL_BYTE
from bxcommon.exceptions import ChecksumError, PayloadLenError
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import crypto
from bxgateway import ont_constants
from bxutils import logging
from bxgateway import log_messages

logger = logging.get_logger(__name__)


class OntMessage(AbstractMessage):
    MESSAGE_TYPE = b"ont"
    HEADER_LENGTH = ont_constants.ONT_HDR_COMMON_OFF

    def __init__(self, magic: Optional[int] = None, command: Optional[bytes] = None, payload_len: Optional[int] = None,
                 buf: Optional[bytearray] = None):
        self.buf = buf
        # pyre-fixme[6]: Expected `Union[bytearray, bytes, memoryview]` for 1st
        #  param but got `Optional[bytearray]`.
        self._memoryview = memoryview(buf)

        # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[int]`.
        magic_num = magic if magic not in ont_constants.ONT_MAGIC_NUMBERS else ont_constants.ONT_MAGIC_NUMBERS[magic]
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        checksum = crypto.double_sha256(self._memoryview[ont_constants.ONT_HDR_COMMON_OFF:payload_len +
                                        ont_constants.ONT_HDR_COMMON_OFF])

        off = 0
        # pyre-fixme[6]: Expected `Union[array.array[typing.Any], bytearray,
        #  memoryview, mmap.mmap]` for 2nd param but got `Optional[bytearray]`.
        struct.pack_into("<L12sL", buf, off, magic_num, command, payload_len)
        off += ont_constants.ONT_HEADER_MINUS_CHECKSUM
        # pyre-fixme[16]: `Optional` has no attribute `__setitem__`.
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
            logger.error(log_messages.PAYLOAD_LENGTH_MISMATCH, payload_length, len(buf))
            raise PayloadLenError("Payload length error raised")
        ref_checksum = crypto.double_sha256(buf[cls.HEADER_LENGTH:cls.HEADER_LENGTH + payload_length])[0:4]
        if checksum != ref_checksum:
            logger.error(log_messages.PACKET_CHECKSUM_MISMATCH, checksum, ref_checksum, buf)
            raise ChecksumError("Checksum error raised", buf)

    @classmethod
    def initialize_class(cls, cls_type, buf, unpacked_args):
        return cls_type(buf=buf)

    def rawbytes(self) -> memoryview:
        assert self.buf is not None
        if self._payload_len is None:
            # pyre-fixme[6]: Expected `Union[array.array[int], bytearray, bytes,
            #  memoryview, mmap.mmap]` for 2nd param but got `Optional[bytearray]`.
            self._payload_len, = struct.unpack_from("<L", self.buf, 16)
        assert self._payload_len is not None
        # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
        # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytearray]`.
        if self._payload_len + ont_constants.ONT_HDR_COMMON_OFF == len(self.buf):
            return self._memoryview
        else:
            # pyre-fixme[6]: Expected `int` for 1st param but got `Optional[int]`.
            return self._memoryview[0:self._payload_len + ont_constants.ONT_HDR_COMMON_OFF]

    def magic(self) -> int:
        if self._magic is None:
            assert self.buf is not None
            # pyre-fixme[6]: Expected `Union[array.array[int], bytearray, bytes,
            #  memoryview, mmap.mmap]` for 2nd param but got `Optional[bytearray]`.
            self._magic, = struct.unpack_from("<L", self.buf)
        return self._magic

    def command(self) -> bytes:
        if self._command is None:
            assert self.buf is not None
            # pyre-fixme[6]: Expected `Union[array.array[int], bytearray, bytes,
            #  memoryview, mmap.mmap]` for 2nd param but got `Optional[bytearray]`.
            self._command = struct.unpack_from("<12s", self.buf, 4)[0].rstrip(MSG_NULL_BYTE)
        assert self._payload_len is not None
        # pyre-fixme[7]: Expected `bytes` but got `Optional[bytes]`.
        return self._command

    def payload_len(self) -> int:
        if self._payload_len is None:
            assert self.buf is not None
            # pyre-fixme[6]: Expected `Union[array.array[int], bytearray, bytes,
            #  memoryview, mmap.mmap]` for 2nd param but got `Optional[bytearray]`.
            self._payload_len, = struct.unpack_from("<L", self.buf, 16)
        assert self._payload_len is not None
        # pyre-fixme[7]: Expected `int` but got `Optional[int]`.
        return self._payload_len

    def payload(self) -> bytearray:
        if self._payload is None:
            assert self.buf is not None
            # pyre-fixme[16]: `Optional` has no attribute `__getitem__`.
            self._payload = self.buf[ont_constants.ONT_HDR_COMMON_OFF:self.payload_len() +
                                     ont_constants.ONT_HDR_COMMON_OFF]
        # pyre-fixme[7]: Expected `bytearray` but got `None`.
        return self._payload

    def checksum(self):
        if self._checksum is None:
            self._checksum = self.buf[ont_constants.ONT_HEADER_MINUS_CHECKSUM:ont_constants.ONT_HDR_COMMON_OFF]
        return self._checksum
