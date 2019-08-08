import binascii
from math import ceil
from typing import List, Optional

from bxcommon.exceptions import ParseError

"""
Utility functions to work with RLP (Recursive Length Prefix) encoding.

https://github.com/ethereum/wiki/wiki/RLP
"""


def encode_int(value):
    """
    Encode integer value into RLP format

    :param value: int value
    :return: RLP encoded bytes
    """

    if value == 0:
        s = b""
    else:
        s = int_to_big_endian(value)

    if len(s) == 1 and safe_ord(s[0]) < 128:
        return s

    return get_length_prefix_str(len(s)) + s


def decode_int(rlp, start):
    """
    Decodes int value from RLP format

    :param rlp: RLP bytes
    :return: tuple (value, length)
    """

    _, value_len, value_start = consume_length_prefix(rlp, start)

    value_bytes = rlp[value_start:value_start + value_len]

    if len(value_bytes) == 0:
        value = 0
    else:
        value = big_endian_to_int(value_bytes)

    return (value, (value_start - start) + value_len)


def get_length_prefix_str(length):
    """
    Calculates length prefix for byte or string

    :param length: length of bytes or string
    :return: length prefix bytes
    """

    if length is None:
        raise ValueError("Argument length is required")

    return get_length_prefix(length, 128)


def get_length_prefix_list(length):
    """
    Calculates length prefix for list

    :param length: length of list
    :return: length prefix bytes
    """

    if length is None:
        raise ValueError("Argument length is required")

    return get_length_prefix(length, 192)


def get_length_prefix(length, offset):
    """Construct the prefix to lists or strings denoting their length.

    :param length: the length of the item in bytes
    :param offset: ``0x80`` when encoding raw bytes, ``0xc0`` when encoding a list
    """

    if length is None:
        raise ValueError("Argument length is required")

    if offset is None:
        raise ValueError("Argument offset is required")

    if length < 56:
        return ascii_chr(offset + length)
    elif length < 256 ** 8:
        length_string = int_to_big_endian(length)
        return ascii_chr(offset + 56 - 1 + len(length_string)) + length_string
    else:
        raise ValueError("Length greater than 256**8")


def consume_length_prefix(rlp, start):
    """Read a length prefix from an RLP string.

    :param rlp: the rlp string to read from
    :param start: the position at which to start reading
    :returns: a tuple ``(type, length, end)``, where ``type`` is either ``str``
              or ``list`` depending on the type of the following payload,
              ``length`` is the length of the payload in bytes, and ``end`` is
              the position of the first payload byte in the rlp string
    """
    if not isinstance(rlp, memoryview):
        raise TypeError("Only memoryview is allowed for RLP content for best performance. Type provided was: {}"
                        .format(type(rlp)))

    if start is None:
        raise ValueError("Argument start is required")

    b0 = safe_ord(rlp[start])
    if b0 < 128:  # single byte
        return (str, 1, start)
    elif b0 < 128 + 56:  # short string
        if b0 - 128 == 1 and safe_ord(rlp[start + 1]) < 128:
            raise ParseError("Encoded as short string although single byte was possible")
        return (str, b0 - 128, start + 1)
    elif b0 < 192:  # long string
        ll = b0 - 128 - 56 + 1
        if rlp[start + 1:start + 2] == b"\x00":
            raise ParseError("Length starts with zero bytes")
        l = big_endian_to_int(rlp[start + 1:start + 1 + ll])
        if l < 56:
            raise ParseError("Long string prefix used for short string")
        return (str, l, start + 1 + ll)
    elif b0 < 192 + 56:  # short list
        return (list, b0 - 192, start + 1)
    else:  # long list
        ll = b0 - 192 - 56 + 1
        if rlp[start + 1:start + 2] == b"\x00":
            raise ParseError("Length starts with zero bytes")
        l = big_endian_to_int(rlp[start + 1:start + 1 + ll])
        if l < 56:
            raise ParseError("Long list prefix used for short list")
        return (list, l, start + 1 + ll)


def big_endian_to_int(value):
    """
    Convert big endian to int

    :param value: big ending value
    :return: int value
    """

    return int.from_bytes(value, byteorder="big")


def int_to_big_endian(value):
    """
    Converts int to big endian

    :param value: int value
    :return: big ending value
    """

    byte_length = max(ceil(value.bit_length() / 8), 1)
    return (value).to_bytes(byte_length, byteorder="big")


def safe_ord(c):
    """
    Returns an integer representing the Unicode code point of the character or int if int argument is passed

    :param c: character or integer
    :return: integer representing the Unicode code point of the character or int if int argument is passed
    """

    if isinstance(c, int):
        return c
    else:
        return ord(c)


def ascii_chr(value):
    """
    Converts byte to ASCII char

    :param value: ASCII code of character
    :return: char
    """

    return bytes([value])


def str_to_bytes(value):
    """
    Converts string to array of bytes

    :param value: string
    :return: array of bytes
    """

    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)

    return bytes(value, "utf-8")


def get_list_items_bytes(list_bytes: memoryview, remove_items_length_prefix: Optional[bool] = False) -> List[memoryview]:
    """
    Parses items from the list
    :param list_bytes: RLP serialized list bytes
    :param remove_items_length_prefix: indicates if length prefix needs to be removed from each item in the list
    :return: list of bytes
    """
    result = []
    offset = 0

    while offset < len(list_bytes):
        _, item_len, item_start = consume_length_prefix(list_bytes, offset)

        item_bytes = list_bytes[offset:item_start + item_len]
        if remove_items_length_prefix:
            item_bytes = remove_length_prefix(item_bytes)

        result.append(item_bytes)
        offset = item_start + item_len

    assert offset == len(list_bytes)

    return result


def get_first_list_field_items_bytes(object_bytes: memoryview, remove_items_length_prefix: Optional[bool] = False) -> List[memoryview]:
    """
    Parses the first field of RLP serialized object as list
    :param object_bytes: byte of RLP serialized object
    :param remove_items_length_prefix: indicates if length prefix needs to be removed from each item in the list
    :return: list of bytes
    """
    list_bytes = remove_length_prefix(object_bytes)
    return get_list_items_bytes(list_bytes, remove_items_length_prefix)


def remove_length_prefix(item_bytes: memoryview) -> memoryview:
    """
    Removes RLP length prefix from the item bytes
    :param item_bytes: serialized item bytes
    :return: item bytes without length prefix
    """

    _, list_itm_len, list_itm_start = consume_length_prefix(item_bytes, 0)
    return item_bytes[list_itm_start:]


def _pack_left(lnum):
    if lnum == 0:
        return b"\0"
    s = hex(lnum)[2:]
    s = s.rstrip("L")
    if len(s) & 1:
        s = "0" + s
    s = binascii.unhexlify(s)
    return s
