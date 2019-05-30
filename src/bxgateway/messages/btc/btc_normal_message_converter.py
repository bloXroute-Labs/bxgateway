import struct
import typing
from collections import deque

from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.utils import crypto, convert, logger
from bxcommon.messages.bloxroute.compact_block_short_ids_serializer import BlockOffsets
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import btc_constants
from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter, get_block_info
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.utils.block_header_info import BlockHeaderInfo
from bxgateway.messages.btc import btc_messages_util


def parse_bx_block_header(
        bx_block: memoryview, block_pieces: typing.Deque[typing.Union[bytearray, memoryview]]
) -> BlockHeaderInfo:
    block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(bx_block)
    short_ids, short_ids_len = compact_block_short_ids_serializer.deserialize_short_ids_from_buffer(
        bx_block,
        block_offsets.short_id_offset
    )

    # Compute block header hash
    block_header_size = \
        block_offsets.block_begin_offset + \
        btc_constants.BTC_HDR_COMMON_OFF + \
        btc_constants.BTC_BLOCK_HDR_SIZE
    block_hash = BtcObjectHash(
        buf=crypto.bitcoin_hash(
            bx_block[
                block_offsets.block_begin_offset + btc_constants.BTC_HDR_COMMON_OFF:
                block_header_size
            ]
        ),
        length=btc_constants.BTC_SHA_HASH_LEN
    )
    offset = block_header_size

    # Add header piece
    txn_count, txn_count_size = btc_messages_util.btc_varint_to_int(bx_block, block_header_size)
    offset += txn_count_size
    block_pieces.append(bx_block[block_offsets.block_begin_offset:offset])
    return BlockHeaderInfo(block_offsets, short_ids, short_ids_len, block_hash, offset, txn_count)


def parse_bx_block_transactions(
        bx_block: memoryview,
        offset: int,
        short_ids: typing.List[int],
        block_offsets: BlockOffsets,
        tx_service: TransactionService,
        block_pieces: typing.Deque[typing.Union[bytearray, memoryview]]
) -> typing.Tuple[typing.List[int], typing.List[Sha256Hash], int]:
    has_missing, unknown_tx_sids, unknown_tx_hashes = \
        tx_service.get_missing_transactions(short_ids)
    if has_missing:
        return unknown_tx_sids, unknown_tx_hashes, offset
    short_tx_index = 0
    output_offset = offset
    while offset < block_offsets.short_id_offset:
        if bx_block[offset] == btc_constants.BTC_SHORT_ID_INDICATOR:
            sid = short_ids[short_tx_index]
            tx_hash, tx, _ = tx_service.get_transaction(sid)
            offset += btc_constants.BTC_SHORT_ID_INDICATOR_LENGTH
            short_tx_index += 1
        else:
            tx_size = btc_messages_util.get_next_tx_size(bx_block, offset)
            tx = bx_block[offset:offset + tx_size]
            offset += tx_size

        block_pieces.append(tx)
        output_offset += len(tx)

    return unknown_tx_sids, unknown_tx_hashes, output_offset


def build_btc_block(
        block_pieces: typing.Deque[typing.Union[bytearray, memoryview]], size: int
) -> typing.Tuple[BlockBtcMessage, int]:
    btc_block = bytearray(size)
    offset = 0
    for piece in block_pieces:
        next_offset = offset + len(piece)
        btc_block[offset:next_offset] = piece
        offset = next_offset
    return BlockBtcMessage(buf=btc_block), offset


class BtcNormalMessageConverter(AbstractBtcMessageConverter):

    def block_to_bx_block(self, btc_block_msg, tx_service):
        """
        Compresses a Bitcoin block's transactions and packs it into a bloXroute block.
        """
        size = 0
        buf = deque()
        short_ids = []
        header = btc_block_msg.header()
        size += len(header)
        buf.append(header)

        for tx in btc_block_msg.txns():
            tx_hash = BtcObjectHash(buf=crypto.double_sha256(tx), length=btc_constants.BTC_SHA_HASH_LEN)
            short_id = tx_service.get_short_id(tx_hash)
            if short_id == constants.NULL_TX_SID:
                buf.append(tx)
                size += len(tx)
            else:
                short_ids.append(short_id)
                buf.append(btc_constants.BTC_SHORT_ID_INDICATOR_AS_BYTEARRAY)
                size += 1

        serialized_short_ids = compact_block_short_ids_serializer.serialize_short_ids_into_bytes(short_ids)
        buf.append(serialized_short_ids)
        size += constants.UL_ULL_SIZE_IN_BYTES
        offset_buf = struct.pack("<Q", size)
        buf.appendleft(offset_buf)
        size += len(serialized_short_ids)

        block = bytearray(size)
        off = 0
        for blob in buf:
            next_off = off + len(blob)
            block[off:next_off] = blob
            off = next_off

        prev_block_hash = convert.bytes_to_hex(btc_block_msg.prev_block().binary)
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))

        block_info = BlockInfo(
            btc_block_msg.txn_count(),
            btc_block_msg.block_hash(),
            bx_block_hash,
            prev_block_hash,
            short_ids
        )
        return block, block_info

    def bx_block_to_block(self, bx_block, tx_service):
        """
        Uncompresses a bx_block from a broadcast bx_block message and converts to a raw BTC bx_block.

        bx_block must be a memoryview, since memoryview[offset] returns a bytearray, while bytearray[offset] returns
        a byte.
        """
        if not isinstance(bx_block, memoryview):
            bx_block = memoryview(bx_block)

        # Initialize tracking of transaction and SID mapping
        block_pieces = deque()
        header_info = parse_bx_block_header(bx_block, block_pieces)
        unknown_tx_sids, unknown_tx_hashes, offset = parse_bx_block_transactions(
            bx_block,
            header_info.offset,
            header_info.short_ids,
            header_info.block_offsets,
            tx_service,
            block_pieces
        )
        total_tx_count = header_info.txn_count

        if not unknown_tx_sids and not unknown_tx_hashes:
            btc_block_msg, offset = build_btc_block(block_pieces, offset)
            logger.debug(
                "Successfully parsed bx_block broadcast message. {0} transactions in bx_block".format(total_tx_count)
            )
        else:
            btc_block_msg = None
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in bx_block: {2}"
                        .format(len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count))
        block_info = get_block_info(
            bx_block,
            header_info.block_hash,
            header_info.short_ids,
            total_tx_count,
            btc_block_msg
        )
        return btc_block_msg, block_info.block_hash, block_info.short_ids, unknown_tx_sids, unknown_tx_hashes
