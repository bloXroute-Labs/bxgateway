import hashlib
import struct
from collections import deque
from typing import List, Optional, NamedTuple, Any, Union

from csiphash import siphash24

from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import crypto, logger
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_HEADER_MINUS_CHECKSUM
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import get_sizeof_btc_varint, pack_int_to_btc_varint
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage


class CompactBlockDecompressionResult(NamedTuple):
    success: bool
    block_btc_message: Optional[BlockBtcMessage]
    block_transactions: Optional[List[memoryview]]
    missing_transactions_indices: Optional[List[int]]


class CompactBlockRecoveryResult(NamedTuple):
    success: bool
    block_btc_message: Optional[BlockBtcMessage]


class CompactBlockRecoveryItem(NamedTuple):
    compact_block_message: CompactBlockBtcMessage
    block_transactions: Optional[List[memoryview]]
    missing_transactions_indices: Optional[List[int]]


def decompress_compact_block(magic: int, msg: CompactBlockBtcMessage,
                             transaction_service: TransactionService) -> CompactBlockDecompressionResult:
    """
    Handle decompression of Bitcoin compact block.
    Decompression converts compact block message to full block message.

    :param magic: network magic number
    :param msg: compact block message
    :param transaction_service: transaction service instance
    :return: instance of object with decompression result data
    """

    sha256_hash = hashlib.sha256()
    sha256_hash.update(msg.block_header())
    sha256_hash.update(msg.short_nonce_buf())
    hex_digest = sha256_hash.digest()
    key = hex_digest[0:16]

    short_ids = msg.short_ids()

    short_id_to_tx_contents = {}

    for tx_hash in transaction_service.iter_transaction_hashes_not_seen_in_block():
        tx_hash_binary = tx_hash.binary[::-1]
        tx_short_id = _compute_short_id(key, tx_hash_binary)
        if tx_short_id in short_ids:
            short_id_to_tx_contents[tx_short_id] = transaction_service.get_transaction_by_hash(tx_hash)
        if len(short_id_to_tx_contents) == len(short_ids):
            break

    block_transactions = []
    missing_transactions_indices = []
    prefilled_txs = msg.prefilled_txns()
    total_txs_count = len(prefilled_txs) + len(short_ids)

    size = 0
    block_msg_parts = deque()

    block_msg_parts.append(msg.block_header())
    size += len(msg.block_header())

    tx_count_size = get_sizeof_btc_varint(total_txs_count)
    tx_count_buf = bytearray(tx_count_size)
    pack_int_to_btc_varint(total_txs_count, tx_count_buf, 0)
    block_msg_parts.append(tx_count_buf)
    size += tx_count_size

    short_ids_iter = iter(short_ids.keys())

    for index in range(total_txs_count):
        if index not in prefilled_txs:
            short_id = next(short_ids_iter)

            if short_id in short_id_to_tx_contents:
                short_tx = short_id_to_tx_contents[short_id]
                block_msg_parts.append(short_tx)
                block_transactions.append(short_tx)
                size += len(short_tx)
            else:
                missing_transactions_indices.append(index)
                block_transactions.append(None)
        else:
            prefilled_tx = prefilled_txs[index]
            block_msg_parts.append(prefilled_tx)
            block_transactions.append(prefilled_tx)
            size += len(prefilled_tx)

    if len(missing_transactions_indices) > 0:
        return CompactBlockDecompressionResult(False, None, block_transactions, missing_transactions_indices)

    msg_header = bytearray(BTC_HDR_COMMON_OFF)
    struct.pack_into("<L12sL", msg_header, 0, magic, BtcMessageType.BLOCK, size)
    block_msg_parts.appendleft(msg_header)
    size += BTC_HDR_COMMON_OFF

    block_msg_bytes = bytearray(size)
    off = 0
    for blob in block_msg_parts:
        next_off = off + len(blob)
        block_msg_bytes[off:next_off] = blob
        off = next_off

    checksum = crypto.bitcoin_hash(block_msg_bytes[BTC_HDR_COMMON_OFF:size])
    block_msg_bytes[BTC_HEADER_MINUS_CHECKSUM:BTC_HDR_COMMON_OFF] = checksum[0:4]

    return CompactBlockDecompressionResult(True, BlockBtcMessage(buf=block_msg_bytes), None, None)


def decompress_recovered_compact_block(magic: int, msg: CompactBlockBtcMessage, block_transactions: List[Any],
                                       missing_indices: List[int],
                                       recovered_transactions: List[Any]) -> CompactBlockRecoveryResult:
    """
    Handle recovery of Bitcoin compact block message.

    :param magic: network magic number
    :param msg: compact block message
    :param block_transactions: block transactions, available and missing, from original decompression attempt
    :param missing_indices: indices of missing transactions in the block, that required recovery
    :param recovered_transactions: recovered missing transactions
    :return: instance of object with recovery result
    """

    if len(missing_indices) != len(recovered_transactions):
        logger.info("Number of transactions missing in compact block does not match number of recovered transactions."
                    "Missing transactions - {}. Recovered transactions - {}", len(missing_indices),
                    len(recovered_transactions))
        return CompactBlockRecoveryResult(False, None)

    for i in range(len(missing_indices)):
        missing_index = missing_indices[i]
        block_transactions[missing_index] = recovered_transactions[i]

    size = 0
    total_txs_count = len(block_transactions)
    block_msg_parts = deque()

    block_msg_parts.append(msg.block_header())
    size += len(msg.block_header())

    tx_count_size = get_sizeof_btc_varint(total_txs_count)
    tx_count_buf = bytearray(tx_count_size)
    pack_int_to_btc_varint(total_txs_count, tx_count_buf, 0)
    block_msg_parts.append(tx_count_buf)
    size += tx_count_size

    for transaction in block_transactions:
        block_msg_parts.append(transaction)
        size += len(transaction)

    msg_header = bytearray(BTC_HDR_COMMON_OFF)
    struct.pack_into("<L12sL", msg_header, 0, magic, BtcMessageType.BLOCK, size)
    block_msg_parts.appendleft(msg_header)
    size += BTC_HDR_COMMON_OFF

    block_msg_bytes = bytearray(size)
    off = 0
    for blob in block_msg_parts:
        next_off = off + len(blob)
        block_msg_bytes[off:next_off] = blob
        off = next_off

    checksum = crypto.bitcoin_hash(block_msg_bytes[BTC_HDR_COMMON_OFF:size])
    block_msg_bytes[BTC_HEADER_MINUS_CHECKSUM:BTC_HDR_COMMON_OFF] = checksum[0:4]

    return CompactBlockRecoveryResult(True, BlockBtcMessage(buf=block_msg_bytes))


def _compute_short_id(key: bytes, tx_hash_binary: Union[bytearray, memoryview]) -> bytes:
    return siphash24(key, bytes(tx_hash_binary))[0:6]
