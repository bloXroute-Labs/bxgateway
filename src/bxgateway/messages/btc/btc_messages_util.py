import socket
import struct

from bxcommon import constants
from bxgateway import btc_constants


def ipaddrport_to_btcbytearray(ip_addr, ip_port):
    try:
        buf = bytearray(btc_constants.BTC_IP_ADDR_PORT_SIZE)
        buf[10] = 0xff
        buf[11] = 0xff
        buf[12:16] = socket.inet_pton(socket.AF_INET, ip_addr)
        buf[16] = ((ip_port >> 8) & 0xFF)
        buf[17] = (ip_port & 0xFF)
    except socket.error:
        try:
            buf = bytearray(btc_constants.BTC_IP_ADDR_PORT_SIZE)
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
def pack_int_to_btc_varint(val, buf, off):
    if val < 253:
        struct.pack_into("B", buf, off, val)
        return 1
    elif val <= 65535:
        struct.pack_into("<BH", buf, off, btc_constants.BTC_VARINT_SHORT_INDICATOR, val)
        return 3
    elif val <= 4294967295:
        struct.pack_into("<BI", buf, off, btc_constants.BTC_VARINT_INT_INDICATOR, val)
        return 5
    else:
        struct.pack_into("<BQ", buf, off, btc_constants.BTC_VARINT_LONG_INDICATOR, val)
        return 9


def get_sizeof_btc_varint(val):
    if val < 253:
        return 1
    elif val <= 65535:
        return 3
    elif val <= 4294967295:
        return 5
    else:
        return 9


def btc_varint_to_int(buf, off):
    """
    Converts a varint to a regular integer in a buffer bytearray.
    https://en.bitcoin.it/wiki/Protocol_documentation#Variable_length_integer
    """
    if not isinstance(buf, memoryview):
        buf = memoryview(buf)

    if buf[off] == btc_constants.BTC_VARINT_LONG_INDICATOR:
        return struct.unpack_from("<Q", buf, off + 1)[0], 9
    elif buf[off] == btc_constants.BTC_VARINT_INT_INDICATOR:
        return struct.unpack_from("<I", buf, off + 1)[0], 5
    elif buf[off] == btc_constants.BTC_VARINT_SHORT_INDICATOR:
        return struct.unpack_from("<H", buf, off + 1)[0], 3
    else:
        return struct.unpack_from("B", buf, off)[0], 1


def get_next_tx_size(buf, off, tail=-1):
    segwit_flag, = struct.unpack_from(">h", buf, off + 4)

    if segwit_flag == btc_constants.TX_SEGWIT_FLAG_VALUE:
        return get_next_segwit_tx_size(buf, off, tail)
    else:
        return get_next_non_segwit_tx_size(buf, off, tail)


def get_next_non_segwit_tx_size(buf, off, tail=-1):
    """
    Returns the size of the next transaction in the.
    Returns -1 if there's a parsing error or the end goes beyond the tail.
    """
    end = off + btc_constants.TX_VERSION_LEN

    io_size, _, _ = _get_tx_io_count_and_size(buf, end, tail)

    if io_size < 0:
        return -1

    end += io_size + btc_constants.TX_LOCK_TIME_LEN

    if end > tail > 0:
        return -1

    return end - off


def get_next_segwit_tx_size(buf, off, tail=-1):
    """
    Returns the size of the next transaction in the.
    Returns -1 if there's a parsing error or the end goes beyond the tail.
    """
    end = off + btc_constants.TX_VERSION_LEN + btc_constants.TX_SEGWIT_FLAG_LEN

    io_size, txin_c, _ = _get_tx_io_count_and_size(buf, end, tail)

    if io_size < 0:
        return -1

    end += io_size

    for _ in range(txin_c):
        witness_count, size = btc_varint_to_int(buf, end)
        end += size

        for _ in range(witness_count):
            witness_len, size = btc_varint_to_int(buf, end)
            end += size + witness_len

    end += btc_constants.TX_LOCK_TIME_LEN

    if end > tail > 0:
        return -1

    return end - off


def _get_tx_io_count_and_size(buf, start, tail):
    end = start

    txin_c, size = btc_varint_to_int(buf, end)
    end += size

    if end > tail > 0:
        return -1

    for _ in range(txin_c):
        end += 36
        script_len, size = btc_varint_to_int(buf, end)
        end += size + script_len + 4

        if end > tail > 0:
            return -1

    txout_c, size = btc_varint_to_int(buf, end)
    end += size
    for _ in range(txout_c):
        end += 8
        script_len, size = btc_varint_to_int(buf, end)
        end += size + script_len

        if end > tail > 0:
            return -1

    return end - start, txin_c, txout_c
