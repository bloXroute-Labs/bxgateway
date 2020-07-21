import socket
from typing import Union, Optional

from bxcommon.utils.blockchain_utils.ont.ont_common_utils import get_txid, ont_varint_to_int
from bxgateway import ont_constants
from bxgateway.messages.btc.btc_messages_util import pack_int_to_btc_varint


def ipport_to_ontbytearray(ip_port: int) -> Optional[bytearray]:
    buf = bytearray(ont_constants.ONT_IP_PORT_SIZE)
    buf[0] = ((ip_port >> 8) & 0xFF)
    buf[1] = (ip_port & 0xFF)
    return buf


def ipaddr_to_ontbytearray(ip_addr: str) -> Optional[bytearray]:
    try:
        buf = bytearray(ont_constants.ONT_IP_ADDR_SIZE)
        buf[10] = 0xff
        buf[11] = 0xff
        buf[12:16] = socket.inet_pton(socket.AF_INET, ip_addr)
    except socket.error:
        try:
            buf = bytearray(ont_constants.ONT_IP_ADDR_SIZE)
            buf[0:16] = socket.inet_pton(socket.AF_INET6, ip_addr)
        except socket.error:
            return None
    return buf


def pack_int_to_ont_varint(val: int, buf: Union[memoryview, bytearray], off: int) -> int:
    return pack_int_to_btc_varint(val, buf, off)


def get_next_tx_size(buffer: Union[memoryview, bytearray], offset: int) -> int:
    if not isinstance(buffer, memoryview):
        buffer = memoryview(buffer)
    _, txid_len = get_txid(buffer[offset:])
    off = txid_len + offset
    sig_length, size = ont_varint_to_int(buffer, off)
    off += size
    for _ in range(sig_length):
        invoke_length, size = ont_varint_to_int(buffer, off)
        off += size + invoke_length
        verify_length, size = ont_varint_to_int(buffer, off)
        off += size + verify_length
    return off - offset
