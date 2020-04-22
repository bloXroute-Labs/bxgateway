import struct
from typing import Optional

from bxgateway import ont_constants
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ont_messages_util import ipaddr_to_ontbytearray


class AddrOntMessage(OntMessage):
    MESSAGE_TYPE = OntMessageType.ADDRESS

    def __init__(self, magic: Optional[int] = None, addrs: Optional[list] = None, buf: Optional[bytearray] = None):
        if addrs is None:
            addrs = []

        if buf is None:
            buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF + len(addrs) * ont_constants.ONT_NODE_ADDR_LEN + 8)
            self.buf = buf

            off = ont_constants.ONT_HDR_COMMON_OFF
            struct.pack_into("<Q", buf, off, len(addrs))
            off += ont_constants.ONT_LONG_LONG_LEN

            for addr in addrs:
                timestamp, service_type, host_ip, sync_port, cons_port, pid = addr
                struct.pack_into("<QQ", buf, off, timestamp, service_type)
                off += ont_constants.ONT_ADDR_TIME_AND_SERV_LEN
                # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for
                #  2nd param but got `Optional[bytearray]`.
                buf[off:off + ont_constants.ONT_IP_ADDR_SIZE] = ipaddr_to_ontbytearray(host_ip)
                off += ont_constants.ONT_IP_ADDR_SIZE
                struct.pack_into("<HHQ", buf, off, sync_port, cons_port, pid)
                off += ont_constants.ONT_ADDR_PORTS_AND_ID_LEN

            super().__init__(magic, self.MESSAGE_TYPE, off - ont_constants.ONT_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None
