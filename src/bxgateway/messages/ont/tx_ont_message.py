import struct
from typing import Optional

from bxcommon.utils import convert
from bxgateway import ont_constants
from bxgateway.messages.ont import ont_messages_util
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.utils.ont.ont_object_hash import OntObjectHash


class TxOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.TRANSACTIONS

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None, txload: Optional[bytes] = None,
                 buf: Optional[bytearray] = None):
        if buf is None:
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(txload) + ont_constants.ONT_CHAR_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<B", buf, off, version)
            off += ont_constants.ONT_CHAR_LEN

            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for 2nd
            #  param but got `Optional[bytes]`.
            buf[off:off + len(txload)] = txload
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += len(txload)

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._txload = self._tx_hash = None

    def __repr__(self):
        return ("TxOntMessage<version: {}, length: {}, tx_hash: {}, rawbytes: {}>"
                .format(self.version(),
                        len(self.rawbytes()),
                        convert.bytes_to_hex(self.tx_hash().binary),
                        convert.bytes_to_hex(
                            self.rawbytes().tobytes())))

    def version(self) -> int:
        if self._version is None:
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._version, = struct.unpack_from("<B", self.buf, off)
        # pyre-fixme[7]: Expected `int` but got `None`.
        return self._version

    def tx(self) -> bytearray:
        return self.payload()

    def tx_hash(self) -> OntObjectHash:
        if self._tx_hash is None:
            self._tx_hash, _ = ont_messages_util.get_txid(self.payload())
        # pyre-fixme[7]: Expected `OntObjectHash` but got `None`.
        return self._tx_hash
