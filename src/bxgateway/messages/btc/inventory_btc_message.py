import struct

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import btcvarint_to_int, pack_int_to_btcvarint


# an inv_vect is an array of (type, hash) tuples
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class InventoryBtcMessage(BtcMessage):
    def __init__(self, magic=None, inv_vect=None, command=None, buf=None):
        if buf is None:
            buf = bytearray(
                BTC_HDR_COMMON_OFF + 9 + 36 * len(inv_vect))  # we conservatively allocate the max space for varint
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            off += pack_int_to_btcvarint(len(inv_vect), buf, off)

            for inv_item in inv_vect:
                struct.pack_into('<L', buf, off, inv_item[0])
                off += 4
                buf[off:off + 32] = inv_item[1].get_big_endian()
                off += 32

            BtcMessage.__init__(self, magic, command, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

    def count(self):
        off = BTC_HDR_COMMON_OFF
        num_items, size = btcvarint_to_int(self.buf, off)
        return num_items

    def __iter__(self):
        off = BTC_HDR_COMMON_OFF
        num_items, size = btcvarint_to_int(self.buf, off)
        off += size
        self._num_items = num_items

        self._items = list()
        for _ in xrange(num_items):
            invtype = struct.unpack_from('<L', self.buf, off)[0]
            off += 4
            yield (invtype, BtcObjectHash(buf=self.buf, offset=off, length=BTC_SHA_HASH_LEN))
            off += 32


class InvBtcMessage(InventoryBtcMessage):
    MESSAGE_TYPE = BtcMessageType.INVENTORY

    def __init__(self, magic=None, inv_vects=None, buf=None):
        InventoryBtcMessage.__init__(self, magic, inv_vects, self.MESSAGE_TYPE, buf)


class GetDataBtcMessage(InventoryBtcMessage):
    MESSAGE_TYPE = BtcMessageType.GET_DATA

    def __init__(self, magic=None, inv_vects=None, buf=None):
        InventoryBtcMessage.__init__(self, magic, inv_vects, self.MESSAGE_TYPE, buf)


class NotFoundBtcMessage(InventoryBtcMessage):
    MESSAGE_TYPE = BtcMessageType.NOT_FOUND

    def __init__(self, magic=None, inv_vects=None, buf=None):
        InventoryBtcMessage.__init__(self, magic, inv_vects, self.MESSAGE_TYPE, buf)
