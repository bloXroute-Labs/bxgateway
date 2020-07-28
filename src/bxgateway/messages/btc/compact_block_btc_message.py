import struct
from typing import List, Dict

from bxcommon.constants import UL_INT_SIZE_IN_BYTES
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils import crypto
from bxcommon.utils.blockchain_utils.btc.btc_common_utils import btc_varint_to_int
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_BLOCK_HDR_SIZE, BTC_SHA_HASH_LEN, \
    BTC_COMPACT_BLOCK_SHORT_ID_LEN, BTC_SHORT_NONCE_SIZE, BTC_VARINT_MIN_SIZE
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import pack_int_to_btc_varint, get_next_tx_size, pack_block_header


class CompactBlockBtcMessage(BtcMessage, AbstractBlockMessage):
    """
    Message used to notify peers about new blocks.
    Blocks are sent using Compact Block message only if peer notified that would like to receive it instead of Inv message
    by sending Send Compact message after initial handshake.
    """

    MESSAGE_TYPE = BtcMessageType.COMPACT_BLOCK

    # pyre-fixme[9]: magic has type `int`; used as `None`.
    # pyre-fixme[9]: version has type `int`; used as `None`.
    # pyre-fixme[9]: prev_block has type `BtcObjectHash`; used as `None`.
    def __init__(self, magic: int = None, version: int = None, prev_block: BtcObjectHash = None,
                 # pyre-fixme[9]: merkle_root has type `BtcObjectHash`; used as `None`.
                 # pyre-fixme[9]: timestamp has type `int`; used as `None`.
                 # pyre-fixme[9]: bits has type `int`; used as `None`.
                 merkle_root: BtcObjectHash = None, timestamp: int = None, bits: int = None,
                 # pyre-fixme[9]: block_nonce has type `int`; used as `None`.
                 # pyre-fixme[9]: short_nonce has type `int`; used as `None`.
                 # pyre-fixme[9]: short_ids has type `List[memoryview]`; used as `None`.
                 block_nonce: int = None, short_nonce: int = None, short_ids: List[memoryview] = None,
                 # pyre-fixme[9]: prefilled_txns has type `List[memoryview]`; used
                 #  as `None`.
                 # pyre-fixme[9]: buf has type `bytearray`; used as `None`.
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
                # pyre-fixme[6]: Expected `Sized` for 1st param but got `int`.
                # pyre-fixme[6]: Expected `Union[typing.Iterable[int], bytes]` for
                #  2nd param but got `int`.
                buf[off:off + len(tx)] = tx
                # pyre-fixme[6]: Expected `Sized` for 1st param but got `int`.
                off += len(tx)

            self.buf = buf

            super(CompactBlockBtcMessage, self).__init__(magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)
        else:
            self.buf = buf
            self._memoryview = memoryview(buf)
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._version = self._prev_block = self._merkle_root = self._timestamp = None
        # pyre-fixme[8]: Attribute has type `int`; used as `None`.
        self._bits: int = None
        # pyre-fixme[8]: Attribute has type `int`; used as `None`.
        self._block_nonce: int = None
        # pyre-fixme[8]: Attribute has type `int`; used as `None`.
        self._short_nonce: int = None
        # pyre-fixme[8]: Attribute has type `int`; used as `None`.
        self._short_id_c: int = None
        self._short_nonce_buf = None
        # pyre-fixme[8]: Attribute has type `Dict[bytes, int]`; used as `None`.
        self._short_ids: Dict[bytes, int] = None
        # pyre-fixme[8]: Attribute has type `Dict[int, memoryview]`; used as `None`.
        self._pre_filled_transactions: Dict[int, memoryview] = None
        self._block_header = None
        # pyre-fixme[8]: Attribute has type `BtcObjectHash`; used as `None`.
        self._hash_val: BtcObjectHash = None
        self._pre_filled_tx_count = None

    def block_header(self) -> memoryview:
        if self._block_header is None:
            self._block_header = self._memoryview[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE]
        # pyre-fixme[7]: Expected `memoryview` but got `None`.
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
        # pyre-fixme[7]: Expected `int` but got `None`.
        return self._version

    def prev_block_hash(self) -> BtcObjectHash:
        if self._prev_block is None:
            off = BTC_HDR_COMMON_OFF + UL_INT_SIZE_IN_BYTES
            self._prev_block = BtcObjectHash(self.buf, off, BTC_SHA_HASH_LEN)
        # pyre-fixme[7]: Expected `BtcObjectHash` but got `None`.
        return self._prev_block

    def merkle_root(self) -> memoryview:
        if self._merkle_root is None:
            off = BTC_HDR_COMMON_OFF + UL_INT_SIZE_IN_BYTES + BTC_SHA_HASH_LEN
            self._merkle_root = self._memoryview[off:off + BTC_SHA_HASH_LEN]
        # pyre-fixme[7]: Expected `memoryview` but got `None`.
        return self._merkle_root

    def timestamp(self) -> int:
        if self._timestamp is None:
            off = BTC_HDR_COMMON_OFF + UL_INT_SIZE_IN_BYTES + 2 * BTC_SHA_HASH_LEN
            self._timestamp, self._bits, self._block_nonce, self._short_nonce = \
                struct.unpack_from("<IIIQ", self.buf, off)

        # pyre-fixme[7]: Expected `int` but got `None`.
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
        # pyre-fixme[7]: Expected `memoryview` but got `None`.
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

    def pre_filled_transactions(self) -> Dict[int, memoryview]:
        if self._pre_filled_transactions is None:
            off = BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE + BTC_SHORT_NONCE_SIZE
            self._short_id_c, size = btc_varint_to_int(self.buf, off)
            off += size + BTC_COMPACT_BLOCK_SHORT_ID_LEN * self._short_id_c
            self._pre_filled_tx_count, size = btc_varint_to_int(self.buf, off)
            off += size

            self._pre_filled_transactions = {}

            index = -1
            # pyre-fixme[6]: Expected `int` for 1st param but got `None`.
            for _ in range(self._pre_filled_tx_count):
                diff, size = btc_varint_to_int(self.buf, off)
                off += size
                size = get_next_tx_size(self.buf, off)
                buf = self._memoryview[off:off + size]
                off += size

                index += diff + 1
                self._pre_filled_transactions[index] = buf

        return self._pre_filled_transactions

    def txns(self) -> List[str]:
        return [tx.hex() for tx in self.short_ids()]
