import socket
import struct

from bxcommon import constants
from bxgateway.btc_constants import BTC_IP_ADDR_PORT_SIZE


def ipaddrport_to_btcbytearray(ip_addr, ip_port):
    try:
        buf = bytearray(BTC_IP_ADDR_PORT_SIZE)
        buf[10] = 0xff
        buf[11] = 0xff
        buf[12:16] = socket.inet_pton(socket.AF_INET, ip_addr)
        buf[16] = ((ip_port >> 8) & 0xFF)
        buf[17] = (ip_port & 0xFF)
    except socket.error:
        try:
            buf = bytearray(BTC_IP_ADDR_PORT_SIZE)
            buf[0:16] = socket.inet_pton(socket.AF_INET6, ip_addr)
            buf[16] = ((ip_port >> 8) & 0xFF)
            buf[17] = (ip_port & 0xFF)
        except socket.error:
            return None
    return buf


# broken
def btcbytearray_to_ipaddrport(btcbytearray):
    if btcbytearray[0:12] == constants.IP_V4_PREFIX[0:12]:
        return socket.inet_ntop(socket.AF_INET, btcbytearray[12:16]), (btcbytearray[16] << 8) | btcbytearray[17]
    else:
        return socket.inet_ntop(socket.AF_INET6, btcbytearray[:16]), (btcbytearray[16] << 8) | btcbytearray[17]


# pack the value into a varint, return how many bytes it took
def pack_int_to_btcvarint(val, buf, off):
    if val < 253:
        struct.pack_into('B', buf, off, val)
        return 1
    elif val <= 65535:
        struct.pack_into('<BH', buf, off, 0xfd, val)
        return 3
    elif val <= 4294967295:
        struct.pack_into('<BI', buf, off, 0xfe, val)
        return 5
    else:
        struct.pack_into('<BQ', buf, off, 0xff, val)
        return 9


def get_sizeof_btcvarint(val):
    if val < 253:
        return 1
    elif val <= 65535:
        return 3
    elif val <= 4294967295:
        return 5
    else:
        return 9


def btcvarint_to_int(buf, off):
    """
    Converts a varint to a regular integer in a buffer bytearray.
    https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer
    """
    if not isinstance(buf, (bytearray, memoryview, bytes)):
        raise ValueError("buf must be of type bytearray, memoryview, or bytes, not {}".format(type(buf)))

    if buf[off] == 0xff:
        return struct.unpack_from('<Q', buf, off + 1)[0], 9
    elif buf[off] == 0xfe:
        return struct.unpack_from('<I', buf, off + 1)[0], 5
    elif buf[off] == 0xfd:
        return struct.unpack_from('<H', buf, off + 1)[0], 3
    else:
        return struct.unpack_from('B', buf, off)[0], 1


def get_next_tx_size(buf, off, tail=-1):
    """
    Returns the size of the next transaction in the.
    Returns -1 if there's a parsing error or the end goes beyond the tail.
    """
    end = off + 4
    txin_c, size = btcvarint_to_int(buf, end)
    end += size

    if end > tail > 0:
        return -1

    for _ in xrange(txin_c):
        end += 36
        script_len, size = btcvarint_to_int(buf, end)
        end += size + script_len + 4

        if end > tail > 0:
            return -1

    txout_c, size = btcvarint_to_int(buf, end)
    end += size
    for _ in xrange(txout_c):
        end += 8
        script_len, size = btcvarint_to_int(buf, end)
        end += size + script_len

        if end > tail > 0:
            return -1

    end += 4

    if end > tail > 0:
        return -1

    return end - off
