import struct
from typing import Optional, List

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ont_messages_util import ont_varint_to_int


class HeadersOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.HEADERS

    def __init__(self, magic: Optional[int] = None, headers: Optional[list] = None, buf: Optional[bytearray] = None):
        if buf is None:
            # pyre-fixme[6]: Expected `Sized` for 1st param but got
            #  `Optional[List[typing.Any]]`.
            headers_length = len(headers)
            # pyre-fixme[16]: `Optional` has no attribute `__getitem__`.
            header_length = len(headers[0])
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + headers_length * header_length +
                            ont_constants.ONT_INT_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<L", buf, off, headers_length)
            off += ont_constants.ONT_INT_LEN
            # pyre-fixme[16]: `Optional` has no attribute `__iter__`.
            for header in headers:
                buf[off:off + len(header)] = header
                off += len(header)

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(self.buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._headers = None
        self._parsed = False

    def headers(self) -> List[memoryview]:
        if not self._parsed:
            self._parsed = True
            off = ont_constants.ONT_HDR_COMMON_OFF
            headers_length, = struct.unpack_from("<L", self.buf, off)
            off += ont_constants.ONT_INT_LEN
            self._headers = []
            for _ in range(headers_length):
                header_start_index = off
                off += ont_constants.ONT_INT_LEN + 3 * ont_constants.ONT_HASH_LEN + \
                       ont_constants.ONT_BLOCK_TIME_HEIGHT_CONS_DATA_LEN
                consensus_payload_length, size = ont_varint_to_int(self.buf, off)
                off += consensus_payload_length + size
                off += ont_constants.ONT_BLOCK_NEXT_BOOKKEEPER_LEN
                header = self._memoryview[header_start_index:off]
                # pyre-fixme[16]: Optional type has no attribute `append`.
                self._headers.append(header)
                bookkeepers_length, size = ont_varint_to_int(self.buf, off)
                off += size
                for _ in range(bookkeepers_length):
                    _, size = ont_varint_to_int(self.buf, off)
                    off += size + ont_constants.ONT_BOOKKEEPER_LEN
                sig_data_length, size = ont_varint_to_int(self.buf, off)
                off += size
                for _ in range(sig_data_length):
                    sig_length, size = ont_varint_to_int(self.buf, off)
                    off += size + sig_length
        headers = self._headers
        assert isinstance(headers, List)
        return headers
