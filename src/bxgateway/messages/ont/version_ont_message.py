import struct
import time
from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class VersionOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.VERSION

    def __init__(self, magic: Optional[int] = None, version: Optional[int] = None, sync_port: Optional[int] = None,
                 http_info_port: Optional[int] = None, cons_port: Optional[int] = None, cap: Optional[bytes] = None,
                 nonce: Optional[int] = None, start_height: Optional[int] = None, relay: Optional[int] = None,
                 is_consensus: Optional[bool] = None, soft_version: Optional[bytes] = None,
                 services=ont_constants.ONT_NODE_SERVICES, buf: Optional[bytearray] = None):

        if buf is None:
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            buf = bytearray(ont_constants.ONT_VERSION_MSG_LEN + len(soft_version))
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<IQQ", buf, off, version, services, int(time.time()))
            off += ont_constants.ONT_VERSION_VER_SERV_TIME_LEN
            struct.pack_into("<HHH", buf, off, sync_port, http_info_port, cons_port)
            off += ont_constants.ONT_VERSION_PORTS_LEN
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            struct.pack_into("<32sQQB?%dp" % (len(soft_version) + 1,), buf, off, cap, nonce,
                             start_height, relay, is_consensus, soft_version)
            # pyre-fixme[6]: Expected `Sized` for 1st param but got `Optional[bytes]`.
            off += ont_constants.ONT_VERSION_LOAD_LEN + len(soft_version)

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._memoryview = memoryview(buf)
        self._version = self._services = self._timestamp = self._sync_port = self._http_info_port = None
        self._cons_port = self._cap = self._nonce = self._start_height = self._relay = self._is_consensus = None
        self._soft_version = None
