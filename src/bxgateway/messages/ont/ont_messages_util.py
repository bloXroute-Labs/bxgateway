import socket
from typing import Union, Optional, Tuple

from bxcommon.utils import crypto

from bxgateway import ont_constants
from bxgateway.utils.ont.ont_object_hash import OntObjectHash
from bxgateway.messages.btc.btc_messages_util import btc_varint_to_int, pack_int_to_btc_varint


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


def ont_varint_to_int(buf: Union[memoryview, bytearray], off: int) -> Tuple[int, int]:
    return btc_varint_to_int(buf, off)


def get_txid(buffer: Union[memoryview, bytearray]) -> Tuple[OntObjectHash, int]:
    if not isinstance(buffer, memoryview):
        buffer = memoryview(buffer)
    off = 1
    txtype = buffer[off:off + 1]
    off += 1 + 40
    # Deploy type
    if txtype == ont_constants.ONT_TX_DEPLOY_TYPE_INDICATOR_AS_BYTEARRAY:
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size
        off += 1
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size
    # Invoke type
    elif txtype == ont_constants.ONT_TX_INVOKE_TYPE_INDICATOR_AS_BYTEARRAY:
        buffer_length, size = ont_varint_to_int(buffer, off)
        off += buffer_length + size

    _, size = ont_varint_to_int(buffer, off)
    off += size

    return OntObjectHash(buf=crypto.double_sha256(buffer[:off]), length=ont_constants.ONT_HASH_LEN), off


def get_next_tx_size(buffer: Union[memoryview, bytearray], offset: int) -> int:
    _, start = get_txid(buffer[offset:])
    off = start
    sig_length, size = ont_varint_to_int(buffer, off)
    off += size
    for _ in range(sig_length):
        invoke_length, size = ont_varint_to_int(buffer, off)
        off += size + invoke_length
        verify_length, size = ont_varint_to_int(buffer, off)
        off += size + verify_length
    start += off
    return start - offset
