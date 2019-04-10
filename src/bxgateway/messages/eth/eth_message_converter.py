import datetime
import struct
import time
from collections import deque

from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import logger, convert, crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.eth import crypto_utils
from bxgateway.utils.eth import rlp_utils


class EthMessageConverter(AbstractMessageConverter):

    def tx_to_bx_txs(self, tx_msg, network_num):
        """
        Converts Ethereum transactions message to array of internal transaction messages

        The code is optimized and does not make copies of bytes

        :param tx_msg: Ethereum transaction message
        :param network_num: blockchain network number
        :return: array of tuples (transaction message, transaction hash, transaction bytes)
        """

        if not isinstance(tx_msg, TransactionsEthProtocolMessage):
            raise TypeError("TransactionsEthProtocolMessage is expected for arg tx_msg but was {0}"
                            .format(type(tx_msg)))
        bx_tx_msgs = []

        msg_bytes = memoryview(tx_msg.rawbytes())

        _, length, start = rlp_utils.consume_length_prefix(msg_bytes, 0)
        txs_bytes = msg_bytes[start:]

        tx_start_index = 0

        while True:
            _, tx_item_length, tx_item_start = rlp_utils.consume_length_prefix(txs_bytes, tx_start_index)
            tx_bytes = txs_bytes[tx_start_index:tx_item_start + tx_item_length]
            tx_hash_bytes = crypto_utils.keccak_hash(tx_bytes)
            msg_hash = Sha256Hash(tx_hash_bytes)
            bx_tx_msg = TxMessage(tx_hash=msg_hash, network_num=network_num, tx_val=tx_bytes)
            bx_tx_msgs.append((bx_tx_msg, msg_hash, tx_bytes))

            tx_start_index = tx_item_start + tx_item_length

            if tx_start_index == len(txs_bytes):
                break

        return bx_tx_msgs

    def bx_tx_to_tx(self, bx_tx_msg):
        """
        Converts internal transaction message to Ethereum transactions message

        The code is optimized and does not make copies of bytes

        :param bx_tx_msg: internal transaction message
        :return: Ethereum transactions message
        """

        if not isinstance(bx_tx_msg, TxMessage):
            raise TypeError("Type TxMessage is expected for bx_tx_msg arg but was {0}"
                            .format(type(bx_tx_msg)))

        size = 0

        tx_bytes = bx_tx_msg.tx_val()
        size += len(tx_bytes)

        txs_prefix = rlp_utils.get_length_prefix_list(size)
        size += len(txs_prefix)

        buf = bytearray(size)

        buf[0:len(txs_prefix)] = txs_prefix
        buf[len(txs_prefix):] = tx_bytes

        return TransactionsEthProtocolMessage(buf)

    def block_to_bx_block(self, block_msg, tx_service):
        """
        Convert Ethereum new block message to internal broadcast message with transactions replaced with short ids

        The code is optimized and does not make copies of bytes

        :param block_msg: Ethereum new block message
        :param tx_service: Transactions service
        :return: Internal broadcast message bytes (bytearray), tuple (txs count, previous block hash)
        """

        if not isinstance(block_msg, NewBlockEthProtocolMessage):
            raise TypeError("Type NewBlockEthProtocolMessage is expected for arg block_msg but was {0}"
                            .format(type(block_msg)))

        compress_start_datetime = datetime.datetime.utcnow()
        compress_start_timestamp = time.time()
        msg_bytes = memoryview(block_msg.rawbytes())

        _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(msg_bytes, 0)
        block_msg_bytes = msg_bytes[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

        _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
        block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

        _, diff_itm_len, diff_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes,
                                                                          block_itm_start + block_itm_len)
        diff_full_bytes = block_msg_bytes[block_itm_start + block_itm_len:diff_itm_start + diff_itm_len]

        _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
        block_hdr_full_bytes = block_itm_bytes[0:block_hdr_itm_start + block_hdr_itm_len]
        block_hdr_bytes = block_itm_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

        _, prev_block_itm_len, prev_block_itm_start = rlp_utils.consume_length_prefix(block_hdr_bytes, 0)
        prev_block_bytes = block_hdr_bytes[prev_block_itm_start:prev_block_itm_start + prev_block_itm_len]

        _, txs_itm_len, txs_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes,
                                                                        block_hdr_itm_start + block_hdr_itm_len)
        txs_bytes = block_itm_bytes[txs_itm_start:txs_itm_start + txs_itm_len]

        uncles_full_bytes = block_itm_bytes[txs_itm_start + txs_itm_len:]

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

            short_tx_content_size = len(is_full_tx_bytes) + \
                                    len(tx_content_prefix) + len(tx_content_bytes)

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

        buf.append(uncles_full_bytes)
        content_size += len(uncles_full_bytes)

        buf.append(diff_full_bytes)
        content_size += len(diff_full_bytes)

        compact_block_msg_prefix = rlp_utils.get_length_prefix_list(content_size)
        buf.appendleft(compact_block_msg_prefix)
        content_size += len(compact_block_msg_prefix)

        short_ids_bytes = compact_block_short_ids_serializer.serialize_short_ids_into_bytes(used_short_ids)
        buf.append(short_ids_bytes)
        content_size += constants.C_SIZE_T_SIZE_IN_BYTES
        offset_buf = struct.pack("@Q", content_size)
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
        return block, block_info

    def bx_block_to_block(self, block, tx_service):
        """
        Converts internal broadcast message to Ethereum new block message

        The code is optimized and does not make copies of bytes

        :param bx_block_msg: internal broadcast message bytes
        :param tx_service: Transactions service
        :return: tuple (new block message, block hash, unknown transaction short id, unknown transaction hashes)
        """

        if not isinstance(block, (bytearray, memoryview)):
            raise TypeError("Type bytearray is expected for arg block_bytes but was {0}"
                            .format(type(block)))

        decompress_start_datetime = datetime.datetime.utcnow()
        decompress_start_timestamp = time.time()

        block_msg_bytes = block if isinstance(block, memoryview) else memoryview(block)

        block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(block)
        short_ids, short_ids_bytes_len = compact_block_short_ids_serializer.deserialize_short_ids_from_buffer(
            block,
            block_offsets.short_id_offset
        )

        block_bytes = block_msg_bytes[block_offsets.block_begin_offset: block_offsets.short_id_offset]

        _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_bytes, 0)
        block_itm_bytes = block_bytes[block_itm_start:]

        _, block_hdr_len, block_hdr_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
        full_hdr_bytes = block_itm_bytes[0:block_hdr_start + block_hdr_len]

        block_hash_bytes = crypto_utils.keccak_hash(full_hdr_bytes)
        block_hash = Sha256Hash(block_hash_bytes)

        _, block_txs_len, block_txs_start = rlp_utils.consume_length_prefix(block_itm_bytes,
                                                                            block_hdr_start + block_hdr_len)
        txs_bytes = block_itm_bytes[block_txs_start:block_txs_start + block_txs_len]

        _, block_uncles_len, block_uncles_start = rlp_utils.consume_length_prefix(block_itm_bytes,
                                                                                  block_txs_start + block_txs_len)
        full_uncles_bytes = block_itm_bytes[block_txs_start + block_txs_len:block_uncles_start + block_uncles_len]

        full_diff_bytes = block_itm_bytes[block_uncles_start + block_uncles_len:]

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

            _, tx_content_len, tx_content_start = rlp_utils.consume_length_prefix(tx_bytes,
                                                                                  is_full_tx_start + is_full_tx_len)
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

            buf.append(full_uncles_bytes)
            content_size += len(full_uncles_bytes)

            block_len_prefix = rlp_utils.get_length_prefix_list(content_size)
            buf.appendleft(block_len_prefix)
            content_size += len(block_len_prefix)

            buf.append(full_diff_bytes)
            content_size += len(full_diff_bytes)

            msg_len_prefix = rlp_utils.get_length_prefix_list(content_size)
            buf.appendleft(msg_len_prefix)

            block_msg_bytes = bytearray(content_size)
            off = 0
            for blob in buf:
                next_off = off + len(blob)
                block_msg_bytes[off:next_off] = blob
                off = next_off

            block_msg = NewBlockEthProtocolMessage(block_msg_bytes)
            logger.debug("Successfully parsed block broadcast message. {0} transactions in block"
                         .format(tx_count))

            bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))
            compressed_size = len(block)

            block_info = BlockInfo(block_hash, short_ids, decompress_start_datetime, datetime.datetime.utcnow(),
                                   (time.time() - decompress_start_timestamp) * 1000, tx_count, bx_block_hash,
                                   convert.bytes_to_hex(block_msg.get_previous_block().binary),
                                   len(block_msg.rawbytes()), compressed_size,
                                   100 - float(compressed_size) / content_size * 100)

            return block_msg, block_info, unknown_tx_sids, unknown_tx_hashes
        else:
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in block: {2}"
                        .format(len(unknown_tx_sids), len(unknown_tx_hashes), tx_count))

            return None, BlockInfo(block_hash, short_ids, decompress_start_datetime, datetime.datetime.utcnow(),
                                   (time.time() - decompress_start_timestamp) * 1000, None, None, None, None, None,
                                   None), unknown_tx_sids, unknown_tx_hashes
