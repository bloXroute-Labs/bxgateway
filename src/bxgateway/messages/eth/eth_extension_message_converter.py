import time
from collections import deque
import datetime
from typing import Tuple, Optional, Union, List

import task_pool_executor as tpe

from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.utils import convert, crypto
from bxcommon.utils.blockchain_utils.bdn_tx_to_bx_tx import bdn_tx_to_bx_tx
from bxcommon.utils.blockchain_utils.eth.eth_common_util import raw_tx_to_bx_tx
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.proxy import task_pool_proxy
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.utils.errors import message_conversion_error
from bxgateway.utils.eth import rlp_utils, crypto_utils
from bxgateway.utils.block_info import BlockInfo
from bxgateway import eth_constants
from bxgateway.abstract_message_converter import AbstractMessageConverter, BlockDecompressionResult
from bxutils import logging

logger = logging.get_logger(__name__)


class EthExtensionMessageConverter(AbstractMessageConverter):

    DEFAULT_BLOCK_SIZE = eth_constants.ETH_DEFAULT_BLOCK_SIZE
    MINIMAL_SUB_TASK_TX_COUNT = eth_constants.ETH_MINIMAL_SUB_TASK_TX_COUNT

    def __init__(self):
        self._last_recovery_idx: int = 0
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)
        # self.decompression_tasks = TaskQueueProxy(self._create_decompression_task)

    def tx_to_bx_txs(self, tx_msg, network_num, quota_type: Optional[QuotaType] = None) ->\
            List[Tuple[TxMessage, Sha256Hash, Union[bytearray, memoryview]]]:
        """
        Converts Ethereum transactions message to array of internal transaction messages

        The code is optimized and does not make copies of bytes

        :param tx_msg: Ethereum transaction message
        :param network_num: blockchain network number
        :param quota_type: the quota type to assign to the BDN transaction.
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
            bx_tx, tx_item_length, tx_item_start = raw_tx_to_bx_tx(txs_bytes, tx_start_index, network_num, quota_type)
            bx_tx_msgs.append((bx_tx, bx_tx.message_hash(), bx_tx.tx_val()))

            tx_start_index = tx_item_start + tx_item_length

            if tx_start_index == len(txs_bytes):
                break

        return bx_tx_msgs

    def bdn_tx_to_bx_tx(
            self,
            raw_tx: Union[bytes, bytearray, memoryview],
            network_num: int,
            quota_type: Optional[QuotaType] = None
    ) -> TxMessage:
        return bdn_tx_to_bx_tx(raw_tx, network_num, quota_type)

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

    def block_to_bx_block(self, block_msg: InternalEthBlockInfo, tx_service: ExtensionTransactionService) -> Tuple[memoryview, BlockInfo]:
        compress_start_datetime = datetime.datetime.utcnow()
        compress_start_timestamp = time.time()
        self._default_block_size = max(self._default_block_size, len(block_msg.rawbytes()))
        tsk = self.compression_tasks.borrow_task()
        tsk.init(tpe.InputBytes(block_msg.rawbytes()), tx_service.proxy)
        try:
            task_pool_proxy.run_task(tsk)
        except tpe.AggregatedException as e:
            self.compression_tasks.return_task(tsk)
            raise message_conversion_error.eth_block_compression_error(block_msg.block_hash(), e)
        bx_block = tsk.bx_block()
        block = memoryview(bx_block)
        compressed_size = len(block)
        original_size = len(block_msg.rawbytes())
        block_hash = block_msg.block_hash()

        block_info = BlockInfo(
            block_hash,
            tsk.short_ids(),
            compress_start_datetime,
            datetime.datetime.utcnow(),
            (time.time() - compress_start_timestamp) * 1000,
            tsk.txn_count(),
            tsk.compressed_block_hash().hex_string(),
            tsk.prev_block_hash().hex_string(),
            original_size,
            compressed_size,
            100 - float(compressed_size) / original_size * 100
        )
        self.compression_tasks.return_task(tsk)
        return block, block_info

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

        _, block_txs_len, block_txs_start = rlp_utils.consume_length_prefix(block_itm_bytes,
                                                                            block_hdr_start + block_hdr_len)
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

    def _create_compression_task(self) -> tpe.EthBlockCompressionTask:
        return tpe.EthBlockCompressionTask(self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT)

    # def _create_decompression_task(self) -> tpe.BtcBlockDecompressionTask:
    #     return tpe.BtcBlockDecompressionTask(
    #         self._default_block_size, self.MINIMAL_SUB_TASK_TX_COUNT
    #     )
