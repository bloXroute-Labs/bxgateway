from typing import List

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN, BTC_VARINT_MIN_SIZE
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import btc_varint_to_int, pack_int_to_btc_varint
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class GetBlockTransactionsBtcMessage(BtcMessage):
    """
    Message used to request from peer transactions in a Compact Block that were not found in memory pool
    """

    MESSAGE_TYPE = BtcMessageType.GET_BLOCK_TRANSACTIONS

    def __init__(self, magic: int = None, block_hash: BtcObjectHash = None, indices: List[int] = None,
                 buf: memoryview = None):
        if buf is None:
            buf = bytearray(
                BTC_HDR_COMMON_OFF + BTC_SHA_HASH_LEN + BTC_VARINT_MIN_SIZE + BTC_VARINT_MIN_SIZE * len(indices))
            off = BTC_HDR_COMMON_OFF

            buf[off:off + BTC_SHA_HASH_LEN] = block_hash.get_big_endian()
            off += BTC_SHA_HASH_LEN

            off += pack_int_to_btc_varint(len(indices), buf, off)

            last_real_index = -1
            for real_index in indices:
                diff = real_index - last_real_index - 1
                last_real_index = real_index
                off += pack_int_to_btc_varint(diff, buf, off)

            self.buf = buf
            super(GetBlockTransactionsBtcMessage, self).__init__(magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF,
                                                                 buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._block_hash = self._indices = None

    def block_hash(self) -> BtcObjectHash:
        if self._block_hash is None:
            self._block_hash = BtcObjectHash(buf=self.buf, offset=BTC_HDR_COMMON_OFF, length=BTC_SHA_HASH_LEN)

        return self._block_hash

    def indices(self) -> List[int]:
        if self._indices is None:
            off = BTC_HDR_COMMON_OFF + 32
            num_indices, size = btc_varint_to_int(self.buf, off)
            off += size
            self._indices = []

            index = -1
            for _ in range(num_indices):
                diff_index, size = btc_varint_to_int(self.buf, off)
                index += diff_index + 1
                self._indices.append(index)
                off += size

        return self._indices
