import socket
import struct

from bxcommon import constants
from bxcommon.constants import UL_INT_SIZE_IN_BYTES
from bxcommon.utils.blockchain_utils.btc import btc_common_utils
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxgateway import btc_constants
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN


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


def get_next_tx_size(buf, off, tail=-1):
    segwit_flag = btc_common_utils.is_segwit(buf, off)

    if segwit_flag:
        return get_next_segwit_tx_size(buf, off, tail)
    else:
        return get_next_non_segwit_tx_size(buf, off, tail)


def get_next_non_segwit_tx_size(buf, off, tail=-1):
    """
    Returns the size of the next transaction in the.
    Returns -1 if there's a parsing error or the end goes beyond the tail.
    """
    end = off + btc_constants.TX_VERSION_LEN

    io_size, _, _ = btc_common_utils.get_tx_io_count_and_size(buf, end, tail)

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

    io_size, txin_c, _ = btc_common_utils.get_tx_io_count_and_size(buf, end, tail)

    if io_size < 0:
        return -1

    end += io_size

    for _ in range(txin_c):
        witness_count, size = btc_common_utils.btc_varint_to_int(buf, end)
        end += size

        for _ in range(witness_count):
            witness_len, size = btc_common_utils.btc_varint_to_int(buf, end)
            end += size + witness_len

    end += btc_constants.TX_LOCK_TIME_LEN

    if end > tail > 0:
        return -1

    return end - off


def pack_block_header(buffer: bytearray, version: int, prev_block: BtcObjectHash, merkle_root: BtcObjectHash,
                      timestamp: int, bits: int, block_nonce: int) -> int:
    """
    Packs Bitcoin block header into beginning of buffer
    :param buffer: buffer
    :param version: version
    :param prev_block: previous block hash
    :param merkle_root: merkle tree root hash
    :param timestamp: timestamp
    :param bits: bits
    :param block_nonce: block nonce
    :return: length of header
    """

    off = BTC_HDR_COMMON_OFF
    struct.pack_into("<I", buffer, off, version)
    off += UL_INT_SIZE_IN_BYTES
    buffer[off:off + BTC_SHA_HASH_LEN] = prev_block.get_little_endian()
    off += BTC_SHA_HASH_LEN
    buffer[off:off + BTC_SHA_HASH_LEN] = merkle_root.get_little_endian()
    off += BTC_SHA_HASH_LEN
    struct.pack_into("<III", buffer, off, timestamp, bits, block_nonce)
    off += 3 * UL_INT_SIZE_IN_BYTES

    return off

