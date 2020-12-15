import base64
import json
import struct
import time
from collections import deque
from datetime import datetime
from typing import Tuple, List, Deque, Union

from bxutils import logging
from bxutils.encoding.json_encoder import EnhancedJSONEncoder

from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.messages.bloxroute.compact_block_short_ids_serializer import BlockOffsets
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert, crypto
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import ont_constants, log_messages
from bxgateway.abstract_message_converter import BlockDecompressionResult, finalize_block_bytes
from bxgateway.messages.ont import ont_messages_util
from bxgateway.messages.ont.abstract_ont_message_converter import AbstractOntMessageConverter, get_block_info
from bxgateway.messages.ont.consensus_ont_message import OntConsensusMessage, ConsensusMsgPayload
from bxgateway.utils.block_header_info import BlockHeaderInfo
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.errors import message_conversion_error

logger = logging.get_logger(__name__)


def parse_bx_block_header(bx_block: memoryview, block_pieces: Deque[Union[bytearray, memoryview]]) -> \
        BlockHeaderInfo:
    block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(bx_block)
    short_ids, short_ids_len = compact_block_short_ids_serializer.deserialize_short_ids_from_buffer(
        bx_block,
        block_offsets.short_id_offset
    )

    block_hash = OntObjectHash(binary=bx_block[block_offsets.block_begin_offset + 1:
                                               block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN + 1])
    offset = block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN + 1
    txn_count, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN
    payload_tail_len, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN + payload_tail_len
    owner_and_signature_len, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN + owner_and_signature_len
    consensus_payload_header_len, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN
    block_pieces.append(bx_block[offset: offset + consensus_payload_header_len])
    offset += consensus_payload_header_len
    block_pieces.append(bx_block[offset: offset + ont_constants.ONT_CHAR_LEN])
    offset += ont_constants.ONT_CHAR_LEN
    block_pieces.append(bx_block[offset: offset + ont_constants.ONT_INT_LEN])
    offset += ont_constants.ONT_INT_LEN
    block_start_len_and_txn_header_total_len, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN
    block_pieces.append(bx_block[offset: offset + block_start_len_and_txn_header_total_len])
    offset += block_start_len_and_txn_header_total_len

    return BlockHeaderInfo(block_offsets, short_ids, short_ids_len, block_hash, offset, txn_count)


def parse_bx_block_transactions_and_msg_tail(block_hash: Sha256Hash, bx_block: memoryview, offset: int,
                                             short_ids: List[int], block_offsets: BlockOffsets,
                                             tx_service: TransactionService,
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

        assert tx is not None
        block_pieces.append(tx)
        output_offset += len(tx)

    # Add consensus payload tail and owner and signature to block_pieces
    offset = block_offsets.block_begin_offset + ont_constants.ONT_HASH_LEN + ont_constants.ONT_INT_LEN + 1
    payload_tail_len, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN
    block_pieces.append(bx_block[offset: offset + payload_tail_len])
    offset += payload_tail_len
    owner_and_signature_len, = struct.unpack_from("<L", bx_block, offset)
    offset += ont_constants.ONT_INT_LEN
    block_pieces.append(bx_block[offset: offset + owner_and_signature_len])
    offset += owner_and_signature_len

    return unknown_tx_sids, unknown_tx_hashes, output_offset


def build_ont_block(block_pieces: Deque[Union[bytearray, memoryview]]) -> OntConsensusMessage:
    ont_block = bytearray()
    ont_block += block_pieces[0]
    # Construct base 64 encoded data and then add it to ont_block
    consensus_data_type, = struct.unpack_from("<B", block_pieces[1], 0)
    consensus_data_len, = struct.unpack_from("<L", block_pieces[2], 0)
    consensus_data_payload = bytearray(block_pieces[3])
    for i in range(4, len(block_pieces) - 1):
        consensus_data_payload += block_pieces[i]
    consensus_data = ConsensusMsgPayload(consensus_data_type, consensus_data_len,
                                         base64.b64encode(
                                             bytes(consensus_data_payload)
                                         ).decode(constants.DEFAULT_TEXT_ENCODING))
    consensus_msg_payload = json.dumps(consensus_data, cls=EnhancedJSONEncoder, separators=(",", ":"))
    ont_block += bytearray(consensus_msg_payload.encode(constants.DEFAULT_TEXT_ENCODING))
    ont_block += block_pieces[len(block_pieces) - 1]
    return OntConsensusMessage(buf=ont_block)


class OntNormalConsensusMessageConverter(AbstractOntMessageConverter):

    def block_to_bx_block(
        self,
        block_msg: OntConsensusMessage,
        tx_service: TransactionService,
        enable_block_compression: bool,
        min_tx_age_seconds: float
    ) -> Tuple[memoryview, BlockInfo]:
        """
        Pack an Ontology consensus message's transactions into a bloXroute block.
        """
        consensus_msg = block_msg
        compress_start_datetime = datetime.utcnow()
        compress_start_timestamp = time.time()
        size = 0
        buf = deque()
        short_ids = []
        ignored_sids = []
        original_size = len(consensus_msg.rawbytes())

        consensus_payload_header = consensus_msg.consensus_payload_header()
        consensus_payload_header_len = bytearray(ont_constants.ONT_INT_LEN)
        struct.pack_into("<L", consensus_payload_header_len, 0, len(consensus_payload_header))
        size += ont_constants.ONT_INT_LEN
        buf.append(consensus_payload_header_len)
        size += len(consensus_payload_header)
        buf.append(consensus_payload_header)
        consensus_data_type = bytearray(ont_constants.ONT_CHAR_LEN)
        struct.pack_into("<B", consensus_data_type, 0, consensus_msg.consensus_data_type())
        size += ont_constants.ONT_CHAR_LEN
        buf.append(consensus_data_type)
        consensus_data_len = bytearray(ont_constants.ONT_INT_LEN)
        struct.pack_into("<L", consensus_data_len, 0, consensus_msg.consensus_data_len())
        size += ont_constants.ONT_INT_LEN
        buf.append(consensus_data_len)
        block_start_len = consensus_msg.block_start_len_memoryview()
        txn_header = consensus_msg.txn_header()
        block_start_len_and_txn_header_total_len = bytearray(ont_constants.ONT_INT_LEN)
        struct.pack_into("<L", block_start_len_and_txn_header_total_len, 0, len(block_start_len) + len(txn_header))
        size += ont_constants.ONT_INT_LEN
        buf.append(block_start_len_and_txn_header_total_len)
        size += len(block_start_len)
        buf.append(block_start_len)
        size += len(txn_header)
        buf.append(txn_header)
        max_timestamp_for_compression = time.time() - min_tx_age_seconds

        for tx in consensus_msg.txns():
            tx_hash, _ = ont_messages_util.get_txid(tx)
            transaction_key = tx_service.get_transaction_key(tx_hash)
            short_id = tx_service.get_short_id_by_key(transaction_key)
            short_id_assign_time = 0

            if short_id != constants.NULL_TX_SID:
                short_id_assign_time = tx_service.get_short_id_assign_time(short_id)

            if short_id == constants.NULL_TX_SID or \
                    not enable_block_compression or \
                    short_id_assign_time > max_timestamp_for_compression:
                if short_id != constants.NULL_TX_SIDS:
                    ignored_sids.append(short_id)
                buf.append(tx)
                size += len(tx)
            else:
                short_ids.append(short_id)
                buf.append(ont_constants.ONT_SHORT_ID_INDICATOR_AS_BYTEARRAY)
                size += 1

        # Prepend owner and signature, consensus payload tail, tx count and block hash to bx_block
        owner_and_signature = consensus_msg.owner_and_signature()
        owner_and_signature_len = bytearray(ont_constants.ONT_INT_LEN)
        struct.pack_into("<L", owner_and_signature_len, 0, len(owner_and_signature))
        size += len(owner_and_signature)
        buf.appendleft(owner_and_signature)
        size += ont_constants.ONT_INT_LEN
        buf.appendleft(owner_and_signature_len)
        payload_tail = consensus_msg.payload_tail()
        payload_tail_len = bytearray(ont_constants.ONT_INT_LEN)
        struct.pack_into("<L", payload_tail_len, 0, len(payload_tail))
        size += len(payload_tail)
        buf.appendleft(payload_tail)
        size += ont_constants.ONT_INT_LEN
        buf.appendleft(payload_tail_len)
        txn_count = bytearray(ont_constants.ONT_INT_LEN)
        struct.pack_into("<L", txn_count, 0, consensus_msg.txn_count())
        size += ont_constants.ONT_INT_LEN
        buf.appendleft(txn_count)
        block_hash = consensus_msg.block_hash().binary
        size += ont_constants.ONT_HASH_LEN
        buf.appendleft(block_hash)

        is_consensus_msg_buf = struct.pack("?", True)
        buf.appendleft(is_consensus_msg_buf)
        size += 1

        block = finalize_block_bytes(buf, size, short_ids)

        prev_block_hash = convert.bytes_to_hex(consensus_msg.prev_block_hash().binary)
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))

        block_info = BlockInfo(
            consensus_msg.block_hash(),
            short_ids,
            compress_start_datetime,
            datetime.utcnow(),
            (time.time() - compress_start_timestamp) * 1000,
            consensus_msg.txn_count(),
            bx_block_hash,
            prev_block_hash,
            original_size,
            size,
            100 - float(size) / original_size * 100,
            ignored_sids
        )

        return memoryview(block), block_info

    def bx_block_to_block(self, bx_block_msg: memoryview, tx_service: TransactionService) -> BlockDecompressionResult:
        """
        Uncompresses a bx_block from a broadcast bx_block message and converts to a raw Ontology bx_block.

        bx_block must be a memoryview, since memoryview[offset] returns a bytearray, while bytearray[offset] returns
        a byte.
        """
        decompress_start_datetime = datetime.utcnow()
        decompress_start_timestamp = time.time()

        # Initialize tracking of transaction and SID mapping
        block_pieces = deque()
        header_info = parse_bx_block_header(bx_block_msg, block_pieces)
        unknown_tx_sids, unknown_tx_hashes, offset = parse_bx_block_transactions_and_msg_tail(
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
            ont_consensus_msg = build_ont_block(block_pieces)
            logger.debug("Successfully parsed bx_block broadcast message. {} transactions in bx_block", total_tx_count)
        else:
            ont_consensus_msg = None
            logger.warning(log_messages.BLOCK_RECOVERY_NEEDED_ONT_CONSENSUS,
                           len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count)
        block_info = get_block_info(
            bx_block_msg,
            header_info.block_hash,
            header_info.short_ids,
            decompress_start_datetime,
            decompress_start_timestamp,
            total_tx_count,
            ont_consensus_msg
        )
        return BlockDecompressionResult(ont_consensus_msg, block_info, unknown_tx_sids, unknown_tx_hashes)
