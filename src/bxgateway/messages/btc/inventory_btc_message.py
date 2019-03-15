import struct

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import btc_varint_to_int, pack_int_to_btc_varint


# an inv_vect is an array of (type, hash) tuples
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class InventoryType(object):
    MSG_WITNESS_FLAG = 1 << 30

    MSG_TX = 1
    MSG_BLOCK = 2
    MSG_WITNESS_BLOCK = MSG_BLOCK | MSG_WITNESS_FLAG
    MSG_WITNESS_TX = MSG_TX | MSG_WITNESS_FLAG

    @classmethod
    def is_block(cls, inventory_type):
        return inventory_type != 0 and inventory_type != InventoryType.MSG_TX and inventory_type != InventoryType.MSG_WITNESS_TX


class InventoryBtcMessage(BtcMessage):
    def __init__(self, magic=None, inv_vect=None, command=None, request_witness_data=False, buf=None):
        if buf is None:
            buf = bytearray(
                BTC_HDR_COMMON_OFF + 9 + 36 * len(inv_vect))  # we conservatively allocate the max space for varint
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            off += pack_int_to_btc_varint(len(inv_vect), buf, off)

            for inv_item in inv_vect:

                inv_type = inv_item[0]
                if request_witness_data:
                    inv_type = self._inv_type_to_witness_inv_type(inv_item[0])

                struct.pack_into("<L", buf, off, inv_type)
                off += 4
                buf[off:off + 32] = inv_item[1].get_big_endian()
                off += 32

            super(InventoryBtcMessage, self).__init__(magic, command, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

    def count(self):
        off = BTC_HDR_COMMON_OFF
        num_items, size = btc_varint_to_int(self.buf, off)
        return num_items

    def _inv_type_to_witness_inv_type(self, inv_type):
        if inv_type == InventoryType.MSG_TX:
            return InventoryType.MSG_WITNESS_TX

        if inv_type == InventoryType.MSG_BLOCK:
            return InventoryType.MSG_WITNESS_BLOCK

        return inv_type

    def __iter__(self):
        off = BTC_HDR_COMMON_OFF
        num_items, size = btc_varint_to_int(self.buf, off)
        off += size
        self._num_items = num_items

        self._items = list()
        for _ in xrange(num_items):
            invtype = struct.unpack_from("<L", self.buf, off)[0]
            off += 4
            yield (invtype, BtcObjectHash(buf=self.buf, offset=off, length=BTC_SHA_HASH_LEN))
            off += 32


class InvBtcMessage(InventoryBtcMessage):
    MESSAGE_TYPE = BtcMessageType.INVENTORY

    def __init__(self, magic=None, inv_vects=None, buf=None):
        super(InvBtcMessage, self).__init__(magic, inv_vects, self.MESSAGE_TYPE, False, buf)


class GetDataBtcMessage(InventoryBtcMessage):
    MESSAGE_TYPE = BtcMessageType.GET_DATA

    def __init__(self, magic=None, inv_vects=None, request_witness_data=False, buf=None):
        super(GetDataBtcMessage, self).__init__(magic, inv_vects, self.MESSAGE_TYPE, request_witness_data, buf)


class NotFoundBtcMessage(InventoryBtcMessage):
    MESSAGE_TYPE = BtcMessageType.NOT_FOUND

    def __init__(self, magic=None, inv_vects=None, buf=None):
        super(NotFoundBtcMessage, self).__init__(magic, inv_vects, self.MESSAGE_TYPE, False, buf)
