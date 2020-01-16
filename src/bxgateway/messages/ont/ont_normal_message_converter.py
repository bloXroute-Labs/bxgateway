import struct
from collections import deque
from datetime import datetime
import time
from typing import Tuple, List, Deque, Union

from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.messages.bloxroute.compact_block_short_ids_serializer import BlockOffsets
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert, crypto
from bxcommon.utils.object_hash import Sha256Hash

from bxutils import logging

from bxgateway.abstract_message_converter import BlockDecompressionResult
from bxgateway import ont_constants
from bxgateway.messages.ont import ont_messages_util
from bxgateway.messages.ont.abstract_ont_message_converter import AbstractOntMessageConverter, get_block_info
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.block_header_info import BlockHeaderInfo
from bxgateway.utils.errors import message_conversion_error


logger = logging.get_logger(__name__)


def parse_bx_block_header(bx_block: memoryview, block_pieces: Deque[Union[bytearray, memoryview]]) -> \
        BlockHeaderInfo:
    block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(bx_block)
    short_ids, short_ids_len = compact_block_short_ids_serializer.deserialize_short_ids_from_buffer(
        bx_block,
        block_offsets.short_id_offset
    )

    reconstructed_block_message = BlockOntMessage(buf=bx_block[block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN:])
    block_hash = reconstructed_block_message.block_hash()
    txn_count = reconstructed_block_message.txn_count()
    offset = reconstructed_block_message.txn_offset() + block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN

    # Add header piece
    block_pieces.append(bx_block[block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN:offset])
    return BlockHeaderInfo(block_offsets, short_ids, short_ids_len, block_hash, offset, txn_count)


def parse_bx_block_transactions(block_hash: Sha256Hash, bx_block: memoryview, offset: int, short_ids: List[int],
                                block_offsets: BlockOffsets, tx_service: TransactionService,
                                block_pieces: Deque[Union[bytearray, memoryview]]) -> \
        Tuple[List[int], List[Sha256Hash], int]:
    has_missing, unknown_tx_sids, unknown_tx_hashes = tx_service.get_missing_transactions(short_ids)
    if has_missing:
        return unknown_tx_sids, unknown_tx_hashes, offset
    short_tx_index = 0
    output_offset = offset
    while offset < block_offsets.short_id_offset:
        if bx_block[offset] == ont_constants.ONT_SHORT_ID_INDICATOR:
            try:
                sid = short_ids[short_tx_index]
            except IndexError:
                # TODO: implement ont errors, use btc decompression error for now
                raise message_conversion_error.btc_block_decompression_error(
                    block_hash,
                    f"Message is improperly formatted, short id index ({short_tx_index}) "
                    f"exceeded its array bounds (size: {len(short_ids)})"
                )
            tx_hash, tx, _ = tx_service.get_transaction(sid)
            offset += ont_constants.ONT_SHORT_ID_INDICATOR_LENGTH
            short_tx_index += 1
        else:
            tx_size = ont_messages_util.get_next_tx_size(bx_block, offset)
            tx = bx_block[offset:offset + tx_size]
            offset += tx_size

        block_pieces.append(tx)
        output_offset += len(tx)

    merkle_root = bx_block[block_offsets.block_begin_offset:block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN]
    block_pieces.append(merkle_root)

    return unknown_tx_sids, unknown_tx_hashes, output_offset


def build_ont_block(block_pieces: Deque[Union[bytearray, memoryview]], size: int) -> Tuple[BlockOntMessage, int]:
    ont_block = bytearray(size - ont_constants.ONT_HASH_LEN)
    offset = 0
    for piece in block_pieces:
        next_offset = offset + len(piece)
        ont_block[offset:next_offset] = piece
        offset = next_offset
    return BlockOntMessage(buf=ont_block), offset


class OntNormalMessageConverter(AbstractOntMessageConverter):
    def block_to_bx_block(self, block_msg: BlockOntMessage, tx_service: TransactionService) ->\
            Tuple[memoryview, BlockInfo]:
        """
        Compresses a Ontology block's transactions and packs it into a bloXroute block.
        """
        compress_start_datetime = datetime.utcnow()
        compress_start_timestamp = time.time()
        size = 0
        buf = deque()
        short_ids = []
        header = block_msg.txn_header()
        size += len(header)
        buf.append(header)

        for tx in block_msg.txns():
            tx_hash, _ = ont_messages_util.get_txid(tx)
            short_id = tx_service.get_short_id(tx_hash)
            if short_id == constants.NULL_TX_SID:
                buf.append(tx)
                size += len(tx)
            else:
                short_ids.append(short_id)
                buf.append(ont_constants.ONT_SHORT_ID_INDICATOR_AS_BYTEARRAY)
                size += 1

        serialized_short_ids = compact_block_short_ids_serializer.serialize_short_ids_into_bytes(short_ids)
        buf.append(serialized_short_ids)
        size += constants.UL_ULL_SIZE_IN_BYTES

        merkle_root = block_msg.merkle_root()
        buf.appendleft(merkle_root)
        size += ont_constants.ONT_HASH_LEN

        offset_buf = struct.pack("<Q", size)
        buf.appendleft(offset_buf)
        size += len(serialized_short_ids)

        block = bytearray(size)
        off = 0
        for blob in buf:
            next_off = off + len(blob)
            block[off:next_off] = blob
            off = next_off

        prev_block_hash = convert.bytes_to_hex(block_msg.prev_block_hash().binary)
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))
        original_size = len(block_msg.rawbytes())

        block_info = BlockInfo(
            block_msg.block_hash(),
            short_ids,
            compress_start_datetime,
            datetime.utcnow(),
            (time.time() - compress_start_timestamp) * 1000,
            block_msg.txn_count(),
            bx_block_hash,
            prev_block_hash,
            original_size,
            size,
            100 - float(size) / original_size * 100
        )
        return memoryview(block), block_info

    def bx_block_to_block(self, bx_block_msg: memoryview, tx_service: TransactionService) -> BlockDecompressionResult:
        """
        Uncompresses a bx_block from a broadcast bx_block message and converts to a raw Ontology bx_block.

        bx_block must be a memoryview, since memoryview[offset] returns a bytearray, while bytearray[offset] returns
        a byte.
        """
        if not isinstance(bx_block_msg, memoryview):
            bx_block_msg = memoryview(bx_block_msg)

        decompress_start_datetime = datetime.utcnow()
        decompress_start_timestamp = time.time()

        # Initialize tracking of transaction and SID mapping
        block_pieces = deque()
        header_info = parse_bx_block_header(bx_block_msg, block_pieces)
        unknown_tx_sids, unknown_tx_hashes, offset = parse_bx_block_transactions(
            header_info.block_hash,
            bx_block_msg,
            header_info.offset,
            header_info.short_ids,
            header_info.block_offsets,
            tx_service,
            block_pieces
        )
        total_tx_count = header_info.txn_count

        if not unknown_tx_sids and not unknown_tx_hashes:
            ont_block_msg, offset = build_ont_block(block_pieces, offset)
            logger.debug("Successfully parsed bx_block broadcast message. {} transactions in bx_block", total_tx_count)
        else:
            ont_block_msg = None
            logger.warning("Block recovery needed. Missing {} sids, {} tx hashes. Total txs in bx_block: {}",
                           len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count)
        block_info = get_block_info(
            bx_block_msg,
            header_info.block_hash,
            header_info.short_ids,
            decompress_start_datetime,
            decompress_start_timestamp,
            total_tx_count,
            ont_block_msg
        )
        return BlockDecompressionResult(ont_block_msg, block_info, unknown_tx_sids, unknown_tx_hashes)
