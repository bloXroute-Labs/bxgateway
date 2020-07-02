import datetime
import struct
import time
from collections import deque
from typing import Tuple

from bxutils import logging
from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert, crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.abstract_message_converter import BlockDecompressionResult
from bxgateway.messages.eth.eth_abstract_message_converter import EthAbstractMessageConverter
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.eth import crypto_utils
from bxgateway.utils.eth import rlp_utils


logger = logging.get_logger(__name__)


class EthNormalMessageConverter(EthAbstractMessageConverter):

    def __init__(self):
        super().__init__()

    def block_to_bx_block(self, block_msg: InternalEthBlockInfo, tx_service: TransactionService) -> Tuple[
        memoryview, BlockInfo]:
        """
        Convert Ethereum new block message to internal broadcast message with transactions replaced with short ids

        The code is optimized and does not make copies of bytes

        :param block_msg: Ethereum new block message
        :param tx_service: Transactions service
        :return: Internal broadcast message bytes (bytearray), tuple (txs count, previous block hash)
        """

        compress_start_datetime = datetime.datetime.utcnow()
        compress_start_timestamp = time.time()
        msg_bytes = memoryview(block_msg.rawbytes())

        _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(msg_bytes, 0)

        block_msg_bytes = msg_bytes[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

        _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
        block_hdr_full_bytes = block_msg_bytes[0:block_hdr_itm_start + block_hdr_itm_len]
        block_hdr_bytes = block_msg_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

        _, prev_block_itm_len, prev_block_itm_start = rlp_utils.consume_length_prefix(block_hdr_bytes, 0)
        prev_block_bytes = block_hdr_bytes[prev_block_itm_start:prev_block_itm_start + prev_block_itm_len]

        _, txs_itm_len, txs_itm_start = rlp_utils.consume_length_prefix(
            block_msg_bytes, block_hdr_itm_start + block_hdr_itm_len
        )
        txs_bytes = block_msg_bytes[txs_itm_start:txs_itm_start + txs_itm_len]

        remaining_bytes = block_msg_bytes[txs_itm_start + txs_itm_len:]

        used_short_ids = []

        # creating transactions content
        content_size = 0
        buf = deque()

        tx_start_index = 0
        tx_count = 0

        while True:
            if tx_start_index >= len(txs_bytes):
                break

            _, tx_item_length, tx_item_start = rlp_utils.consume_length_prefix(txs_bytes, tx_start_index)
            tx_bytes = txs_bytes[tx_start_index:tx_item_start + tx_item_length]
            tx_hash_bytes = crypto_utils.keccak_hash(tx_bytes)
            tx_hash = Sha256Hash(tx_hash_bytes)
            short_id = tx_service.get_short_id(tx_hash)

            if short_id <= 0:
                is_full_tx_bytes = rlp_utils.encode_int(1)
                tx_content_bytes = tx_bytes
            else:
                is_full_tx_bytes = rlp_utils.encode_int(0)
                used_short_ids.append(short_id)
                tx_content_bytes = bytes()

            tx_content_prefix = rlp_utils.get_length_prefix_str(len(tx_content_bytes))

            short_tx_content_size = len(is_full_tx_bytes) + len(tx_content_prefix) + len(tx_content_bytes)

            short_tx_content_prefix_bytes = rlp_utils.get_length_prefix_list(short_tx_content_size)

            buf.append(short_tx_content_prefix_bytes)
            buf.append(is_full_tx_bytes)
            buf.append(tx_content_prefix)
            buf.append(tx_content_bytes)

            content_size += len(short_tx_content_prefix_bytes) + short_tx_content_size

            tx_start_index = tx_item_start + tx_item_length

            tx_count += 1

        list_of_txs_prefix_bytes = rlp_utils.get_length_prefix_list(content_size)
        buf.appendleft(list_of_txs_prefix_bytes)
        content_size += len(list_of_txs_prefix_bytes)

        buf.appendleft(block_hdr_full_bytes)
        content_size += len(block_hdr_full_bytes)

        buf.append(remaining_bytes)
        content_size += len(remaining_bytes)

        compact_block_msg_prefix = rlp_utils.get_length_prefix_list(content_size)
        buf.appendleft(compact_block_msg_prefix)
        content_size += len(compact_block_msg_prefix)

        short_ids_bytes = compact_block_short_ids_serializer.serialize_short_ids_into_bytes(used_short_ids)
        buf.append(short_ids_bytes)
        content_size += constants.UL_ULL_SIZE_IN_BYTES
        offset_buf = struct.pack("<Q", content_size)
        buf.appendleft(offset_buf)
        content_size += len(short_ids_bytes)

        # Parse it into the bloXroute message format and send it along
        block = bytearray(content_size)
        off = 0
        for blob in buf:
            next_off = off + len(blob)
            block[off:next_off] = blob
            off = next_off

        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))
        original_size = len(block_msg.rawbytes())

        block_info = BlockInfo(block_msg.block_hash(), used_short_ids, compress_start_datetime,
                               datetime.datetime.utcnow(), (time.time() - compress_start_timestamp) * 1000,
                               tx_count, bx_block_hash, convert.bytes_to_hex(prev_block_bytes), original_size,
                               content_size, 100 - float(content_size) / original_size * 100)
        return memoryview(block), block_info

    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        """
        Converts internal broadcast message to Ethereum new block message

        The code is optimized and does not make copies of bytes

        :param bx_block_msg: internal broadcast message bytes
        :param tx_service: Transactions service
        :return: tuple (new block message, block hash, unknown transaction short id, unknown transaction hashes)
        """

        if not isinstance(bx_block_msg, (bytearray, memoryview)):
            raise TypeError("Type bytearray is expected for arg block_bytes but was {0}"
                            .format(type(bx_block_msg)))

        decompress_start_datetime = datetime.datetime.utcnow()
        decompress_start_timestamp = time.time()

        block_msg_bytes = bx_block_msg if isinstance(bx_block_msg, memoryview) else memoryview(bx_block_msg)

        block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(bx_block_msg)
        short_ids, short_ids_bytes_len = compact_block_short_ids_serializer.deserialize_short_ids_from_buffer(
            bx_block_msg,
            block_offsets.short_id_offset
        )

        block_bytes = block_msg_bytes[block_offsets.block_begin_offset: block_offsets.short_id_offset]

        _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_bytes, 0)
        block_itm_bytes = block_bytes[block_itm_start:]

        _, block_hdr_len, block_hdr_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
        full_hdr_bytes = block_itm_bytes[0:block_hdr_start + block_hdr_len]

        block_hash_bytes = crypto_utils.keccak_hash(full_hdr_bytes)
        block_hash = Sha256Hash(block_hash_bytes)


        _, block_txs_len, block_txs_start = rlp_utils.consume_length_prefix(
            block_itm_bytes, block_hdr_start + block_hdr_len
        )
        txs_bytes = block_itm_bytes[block_txs_start:block_txs_start + block_txs_len]

        remaining_bytes = block_itm_bytes[block_txs_start + block_txs_len:]

        # parse statistics variables
        short_tx_index = 0
        unknown_tx_sids = []
        unknown_tx_hashes = []

        # creating transactions content
        content_size = 0
        buf = deque()
        tx_count = 0

        tx_start_index = 0

        while True:
            if tx_start_index >= len(txs_bytes):
                break

            _, tx_itm_len, tx_itm_start = rlp_utils.consume_length_prefix(txs_bytes, tx_start_index)
            tx_bytes = txs_bytes[tx_itm_start:tx_itm_start + tx_itm_len]

            is_full_tx_start = 0
            is_full_tx, is_full_tx_len, = rlp_utils.decode_int(tx_bytes, is_full_tx_start)

            _, tx_content_len, tx_content_start = rlp_utils.consume_length_prefix(
                tx_bytes, is_full_tx_start + is_full_tx_len)
            tx_content_bytes = tx_bytes[tx_content_start:tx_content_start + tx_content_len]
            if is_full_tx:
                tx_bytes = tx_content_bytes
            else:
                short_id = short_ids[short_tx_index]
                tx_hash, tx_bytes, _ = tx_service.get_transaction(short_id)

                if tx_hash is None:
                    unknown_tx_sids.append(short_id)
                elif tx_bytes is None:
                    unknown_tx_hashes.append(tx_hash)

                short_tx_index += 1

            if tx_bytes is not None and not unknown_tx_sids and not unknown_tx_hashes:
                buf.append(tx_bytes)
                content_size += len(tx_bytes)

            tx_count += 1

            tx_start_index = tx_itm_start + tx_itm_len

        if not unknown_tx_sids and not unknown_tx_hashes:

            txs_prefix = rlp_utils.get_length_prefix_list(content_size)
            buf.appendleft(txs_prefix)
            content_size += len(txs_prefix)

            buf.appendleft(full_hdr_bytes)
            content_size += len(full_hdr_bytes)

            buf.append(remaining_bytes)
            content_size += len(remaining_bytes)

            msg_len_prefix = rlp_utils.get_length_prefix_list(content_size)
            buf.appendleft(msg_len_prefix)

            block_msg_bytes = bytearray(content_size)
            off = 0
            for blob in buf:
                next_off = off + len(blob)
                block_msg_bytes[off:next_off] = blob
                off = next_off

            block_msg = InternalEthBlockInfo(block_msg_bytes)
            logger.debug("Successfully parsed block broadcast message. {} "
                         "transactions in block {}", tx_count, block_hash)

            bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(bx_block_msg))
            compressed_size = len(bx_block_msg)

            block_info = BlockInfo(block_hash, short_ids, decompress_start_datetime, datetime.datetime.utcnow(),
                                   (time.time() - decompress_start_timestamp) * 1000, tx_count, bx_block_hash,
                                   convert.bytes_to_hex(block_msg.prev_block_hash().binary),
                                   len(block_msg.rawbytes()), compressed_size,
                                   100 - float(compressed_size) / content_size * 100)

            return BlockDecompressionResult(block_msg, block_info, unknown_tx_sids, unknown_tx_hashes)
        else:
            logger.debug(
                "Block recovery needed for {}. Missing {} sids, {} tx hashes. "
                "Total txs in block: {}",
                block_hash,
                len(unknown_tx_sids),
                len(unknown_tx_hashes),
                tx_count
            )

            return BlockDecompressionResult(
                None,
                BlockInfo(
                    block_hash,
                    short_ids,
                    decompress_start_datetime, datetime.datetime.utcnow(),
                    (time.time() - decompress_start_timestamp) * 1000,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ),
                unknown_tx_sids,
                unknown_tx_hashes
            )
