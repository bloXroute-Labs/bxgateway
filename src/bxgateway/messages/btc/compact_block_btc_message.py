import struct
from typing import List, Dict

from bxcommon.constants import UL_INT_SIZE_IN_BYTES
from bxcommon.utils import crypto
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_BLOCK_HDR_SIZE, BTC_SHA_HASH_LEN, \
    BTC_COMPACT_BLOCK_SHORT_ID_LEN, BTC_SHORT_NONCE_SIZE, BTC_VARINT_MIN_SIZE
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import pack_int_to_btc_varint, btc_varint_to_int, get_next_tx_size, \
    pack_block_header
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class CompactBlockBtcMessage(BtcMessage):
    """
    Message used to notify peers about new blocks.
    Blocks are sent using Compact Block message only if peer notified that would like to receive it instead of Inv message
    by sending Send Compact message after initial handshake.
    """

    MESSAGE_TYPE = BtcMessageType.COMPACT_BLOCK

    def __init__(self, magic: int = None, version: int = None, prev_block: BtcObjectHash = None,
                 merkle_root: BtcObjectHash = None, timestamp: int = None, bits: int = None,
                 block_nonce: int = None, short_nonce: int = None, short_ids: List[memoryview] = None,
                 prefilled_txns: List[memoryview] = None, buf: bytearray = None):

        if buf is None:
            prefilled_tx_size = sum(len(tx) for tx in prefilled_txns)
            buf = bytearray(
                BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE + BTC_SHORT_NONCE_SIZE + 2 * BTC_VARINT_MIN_SIZE + BTC_COMPACT_BLOCK_SHORT_ID_LEN * len(
                    short_ids) + prefilled_tx_size)

            off = pack_block_header(buf, version, prev_block, merkle_root, timestamp, bits, block_nonce)

            struct.pack_into("<Q", buf, off, short_nonce)
            off += BTC_SHORT_NONCE_SIZE

            off += pack_int_to_btc_varint(len(short_ids), buf, off)

            for shortid in short_ids:
                buf[off:off + BTC_COMPACT_BLOCK_SHORT_ID_LEN] = shortid
                off += BTC_COMPACT_BLOCK_SHORT_ID_LEN

            off += pack_int_to_btc_varint(len(prefilled_txns), buf, off)

            for index, tx in prefilled_txns:
                buf[off:off + len(tx)] = tx
                off += len(tx)

            self.buf = buf

            super(CompactBlockBtcMessage, self).__init__(magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._prev_block = self._merkle_root = self._timestamp = None
        self._bits = self._block_nonce = self._short_nonce = self._short_id_c = None
        self._short_nonce_buf = None
        self._short_ids = self._prefilled_txns = None
        self._block_header = self._hash_val = None

    def block_header(self) -> memoryview:
        if self._block_header is None:
            self._block_header = self._memoryview[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE]
        return self._block_header

    def block_hash(self) -> BtcObjectHash:
        if self._hash_val is None:
            raw_hash = crypto.bitcoin_hash(self.block_header())
            self._hash_val = BtcObjectHash(buf=raw_hash, length=BTC_SHA_HASH_LEN)
        return self._hash_val

    def version(self) -> int:
        if self._version is None:
            off = BTC_HDR_COMMON_OFF
            self._version = struct.unpack_from("<I", self.buf, off)[0]
        return self._version

    def prev_block(self) -> BtcObjectHash:
        if self._prev_block is None:
            off = BTC_HDR_COMMON_OFF + UL_INT_SIZE_IN_BYTES
            self._prev_block = BtcObjectHash(self.buf, off, BTC_SHA_HASH_LEN)
        return self._prev_block

    def merkle_root(self) -> memoryview:
        if self._merkle_root is None:
            off = BTC_HDR_COMMON_OFF + UL_INT_SIZE_IN_BYTES + BTC_SHA_HASH_LEN
            self._merkle_root = self._memoryview[off:off + BTC_SHA_HASH_LEN]
        return self._merkle_root

    def timestamp(self) -> int:
        if self._timestamp is None:
            off = BTC_HDR_COMMON_OFF + UL_INT_SIZE_IN_BYTES + 2 * BTC_SHA_HASH_LEN
            self._timestamp, self._bits, self._block_nonce, self._short_nonce = \
                struct.unpack_from("<IIIQ", self.buf, off)

        return self._timestamp

    def bits(self) -> int:
        if self._bits is None:
            self.timestamp()
        return self._bits

    def block_nonce(self) -> int:
        if self._block_nonce is None:
            self.timestamp()
        return self._block_nonce

    def short_nonce(self) -> int:
        if self._short_nonce is None:
            self.timestamp()
        return self._short_nonce

    def short_nonce_buf(self) -> memoryview:
        if self._short_nonce_buf is None:
            start = BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE
            self._short_nonce_buf = self._memoryview[start:start + BTC_SHORT_NONCE_SIZE]
        return self._short_nonce_buf

    def short_ids(self) -> Dict[bytes, int]:
        if self._short_ids is None:
            off = BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE + BTC_SHORT_NONCE_SIZE
            self._short_id_c, size = btc_varint_to_int(self.buf, off)
            off += size
            self._short_ids = {}

            for idx in range(self._short_id_c):
                self._short_ids[bytes(self._memoryview[off:off + BTC_COMPACT_BLOCK_SHORT_ID_LEN])] = idx
                off += BTC_COMPACT_BLOCK_SHORT_ID_LEN

        return self._short_ids

    def prefilled_txns(self) -> Dict[int, memoryview]:
        if self._prefilled_txns is None:
            off = BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE + BTC_SHORT_NONCE_SIZE
            self._short_id_c, size = btc_varint_to_int(self.buf, off)
            off += size + BTC_COMPACT_BLOCK_SHORT_ID_LEN * self._short_id_c
            self._prefill_tx_c, size = btc_varint_to_int(self.buf, off)
            off += size

            self._prefilled_txns = {}

            index = -1
            for _ in range(self._prefill_tx_c):
                diff, size = btc_varint_to_int(self.buf, off)
                off += size
                size = get_next_tx_size(self.buf, off)
                buf = self._memoryview[off:off + size]
                off += size

                index += diff + 1
                self._prefilled_txns[index] = buf

        return self._prefilled_txns
