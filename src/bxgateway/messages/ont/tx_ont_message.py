import struct
from typing import Optional

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxgateway import ont_constants
from bxgateway.messages.ont import ont_messages_util
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class TxOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.TRANSACTIONS

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None, txload: Optional[bytes] = None,
                 buf: Optional[bytearray] = None):
        if buf is None:
            assert version is not None
            assert txload is not None
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(txload) + ont_constants.ONT_CHAR_LEN)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<B", buf, off, version)
            off += ont_constants.ONT_CHAR_LEN

            #  param but got `Optional[bytes]`.
            buf[off:off + len(txload)] = txload
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
                        convert.bytes_to_hex(self.rawbytes().tobytes())
                        )
                )

    def version(self) -> int:
        if self._version is None:
            off = ont_constants.ONT_HDR_COMMON_OFF
            self._version, = struct.unpack_from("<B", self.buf, off)
        version = self._version
        assert isinstance(version, int)
        return version

    def tx(self) -> bytearray:
        return self.payload()

    def tx_hash(self) -> OntObjectHash:
        if self._tx_hash is None:
            self._tx_hash, _ = ont_messages_util.get_txid(self.payload())
        tx_hash = self._tx_hash
        assert isinstance(tx_hash, OntObjectHash)
        return tx_hash
