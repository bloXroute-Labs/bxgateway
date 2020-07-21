import struct
from typing import Iterator, Tuple

from bxutils.logging.log_level import LogLevel

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.btc import btc_common_utils
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxgateway import btc_constants
from bxgateway.messages.btc import btc_messages_util
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType


def _inv_type_to_witness_inv_type(inv_type):
    if inv_type == InventoryType.MSG_TX:
        return InventoryType.MSG_WITNESS_TX

    if inv_type == InventoryType.MSG_BLOCK:
        return InventoryType.MSG_WITNESS_BLOCK

    return inv_type


class InventoryType(object):
    MSG_WITNESS_FLAG = 1 << 30

    MSG_TX = 1
    MSG_BLOCK = 2
    MSG_WITNESS_BLOCK = MSG_BLOCK | MSG_WITNESS_FLAG
    MSG_WITNESS_TX = MSG_TX | MSG_WITNESS_FLAG
    MSG_CMPCT_BLOCK = 4

    @classmethod
    def is_block(cls, inventory_type):
        return inventory_type != 0 and inventory_type != InventoryType.MSG_TX and inventory_type != InventoryType.MSG_WITNESS_TX


class InventoryBtcMessage(BtcMessage):
    def __init__(self, magic=None, inv_vect=None, command=None, request_witness_data=False, buf=None):
        if buf is None:
            buf = bytearray(
                btc_constants.BTC_HDR_COMMON_OFF + 9 + 36 * len(
                    inv_vect))  # we conservatively allocate the max space for varint
            self.buf = buf

            off = btc_constants.BTC_HDR_COMMON_OFF
            off += btc_messages_util.pack_int_to_btc_varint(len(inv_vect), buf, off)

            for inv_item in inv_vect:

                inv_type = inv_item[0]
                if request_witness_data:
                    inv_type = _inv_type_to_witness_inv_type(inv_item[0])

                struct.pack_into("<L", buf, off, inv_type)
                off += 4
                buf[off:off + 32] = inv_item[1].get_big_endian()
                off += 32

            super(InventoryBtcMessage, self).__init__(magic, command, off - btc_constants.BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

    def count(self):
        off = btc_constants.BTC_HDR_COMMON_OFF
        num_items, size = btc_common_utils.btc_varint_to_int(self.buf, off)
        return num_items

    def __iter__(self) -> Iterator[Tuple[int, BtcObjectHash]]:
        off = btc_constants.BTC_HDR_COMMON_OFF
        num_items, size = btc_common_utils.btc_varint_to_int(self.buf, off)
        off += size
        # pyre-fixme[16]: `InventoryBtcMessage` has no attribute `_num_items`.
        self._num_items = num_items

        # pyre-fixme[16]: `InventoryBtcMessage` has no attribute `_items`.
        self._items = list()
        for _ in range(num_items):
            inv_type, = struct.unpack_from("<L", self.buf, off)
            off += 4
            yield (inv_type, BtcObjectHash(buf=self.buf, offset=off, length=btc_constants.BTC_SHA_HASH_LEN))
            off += 32

    def log_level(self):
        return LogLevel.DEBUG

    def __repr__(self):
        return "InventoryBtcMessage<type: {}, length: {}, items: ({})>".format(self.MESSAGE_TYPE, len(self.rawbytes()),
                                                                               self._inv_vector_to_str())

    def _inv_vector_to_str(self):
        return ", ".join(
            map(lambda inv_item: "{}: {}".format(inv_item[0], convert.bytes_to_hex(inv_item[1].binary)), self))


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
