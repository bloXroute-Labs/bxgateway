from typing import List

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN, BTC_VARINT_MIN_SIZE
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import btc_varint_to_int, pack_int_to_btc_varint, get_next_tx_size
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BlockTransactionsBtcMessage(BtcMessage):
    """
    Message used as reply to Get Block Transactions message
    """

    MESSAGE_TYPE = BtcMessageType.BLOCK_TRANSACTIONS

    def __init__(self, magic: int = None, block_hash: BtcObjectHash = None, transactions: List[memoryview] = None,
                 buf: memoryview = None):
        if buf is None:
            total_tx_size = sum(len(tx) for tx in transactions)
            buf = bytearray(BTC_HDR_COMMON_OFF + BTC_SHA_HASH_LEN + BTC_VARINT_MIN_SIZE + total_tx_size)
            off = BTC_HDR_COMMON_OFF

            buf[off:off + BTC_SHA_HASH_LEN] = block_hash.get_big_endian()
            off += BTC_SHA_HASH_LEN

            off += pack_int_to_btc_varint(len(transactions), buf, off)

            for tx in transactions:
                buf[off:off + len(tx)] = tx
                off += len(tx)

            self.buf = buf
            super(BlockTransactionsBtcMessage, self).__init__(magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._block_hash = self._transactions = None

    def block_hash(self) -> BtcObjectHash:
        if self._block_hash is None:
            self._block_hash = BtcObjectHash(buf=self.buf, offset=BTC_HDR_COMMON_OFF, length=BTC_SHA_HASH_LEN)

        return self._block_hash

    def transactions(self) -> List[memoryview]:
        if self._transactions is None:
            self._transactions = []
            off = BTC_HDR_COMMON_OFF + BTC_SHA_HASH_LEN

            tx_len, size = btc_varint_to_int(self.buf, off)
            off += size

            for _ in range(tx_len):
                size = get_next_tx_size(self.buf, off)
                tx = self._memoryview[off:off + size]
                off += size
                self._transactions.append(tx)

        return self._transactions
