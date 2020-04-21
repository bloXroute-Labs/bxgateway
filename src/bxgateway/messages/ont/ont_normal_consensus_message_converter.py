import struct
import time
from collections import deque
from datetime import datetime
from typing import Tuple

from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert, crypto
from bxgateway import ont_constants
from bxgateway.messages.ont import ont_messages_util
from bxgateway.messages.ont.abstract_ont_consensus_message_converter import AbstractOntConsensusMessageConverter
from bxgateway.messages.ont.consensus_ont_message import ConsensusOntMessage, ConsensusMsgPayload
from bxgateway.utils.block_info import BlockInfo
from bxutils import logging

logger = logging.get_logger(__name__)


class OntNormalConsensusMessageConverter(AbstractOntConsensusMessageConverter):

    def block_to_bx_block(
            self, block_msg: ConsensusOntMessage, tx_service: TransactionService
    ) -> Tuple[memoryview, BlockInfo]:
        """
        Compresses a Ontology consensus message's transactions and packs it into a bloXroute block.
        """
        consensus_msg = block_msg
        compress_start_datetime = datetime.utcnow()
        compress_start_timestamp = time.time()
        size = 0
        buf = deque()
        short_ids = []
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

        for tx in consensus_msg.txns():
            tx_hash, _ = ont_messages_util.get_txid(tx)
            short_id = tx_service.get_short_id(tx_hash)
            if short_id == constants.NULL_TX_SID:
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

        prev_block_hash = convert.bytes_to_hex(consensus_msg.prev_block_hash().binary)
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))
        original_size = len(consensus_msg.rawbytes())

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
            100 - float(size) / original_size * 100
        )
        return memoryview(block), block_info
