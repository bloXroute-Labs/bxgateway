from bxcommon.utils import convert, logger
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.proxy.task_queue_proxy import TaskQueueProxy

from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter, get_block_info
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage

import task_pool_executor as tpe  # pyre-ignore for now, figure this out later (stub file or Python wrapper?)


class BtcExtensionMessageConverter(AbstractBtcMessageConverter):

    DEFAULT_BLOCK_SIZE = 621000

    def __init__(self, btc_magic):
        super(BtcExtensionMessageConverter, self).__init__(btc_magic)
        self._default_block_size = self.DEFAULT_BLOCK_SIZE
        self.compression_tasks = TaskQueueProxy(self._create_compression_task)
        self.decompression_tasks = TaskQueueProxy(self._create_decompression_task)

    def block_to_bx_block(self, btc_block_msg, tx_service):
        self._default_block_size = max(self._default_block_size, len(btc_block_msg.buf))
        tsk_proxy = self.compression_tasks.borrow_task()
        tsk = tsk_proxy.tsk
        tsk.init(tpe.InputBytes(btc_block_msg.buf), tx_service.proxy)
        tsk_proxy.run()
        bx_block = tsk.bx_block()
        block = memoryview(bx_block)
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )

        block_info = BlockInfo(
            tsk.txn_count(),
            block_hash,
            tsk.compressed_block_hash().hex_string(),
            tsk.prev_block_hash().hex_string(),
            tsk.short_ids()
        )
        self.compression_tasks.return_task(tsk_proxy)
        return block, block_info

    def bx_block_to_block(self, bx_block, tx_service):
        tsk_proxy = self.decompression_tasks.borrow_task()
        tsk = tsk_proxy.tsk
        tsk.init(tpe.InputBytes(bx_block), tx_service.proxy)
        tsk_proxy.run()
        total_tx_count = tsk.tx_count()
        unknown_tx_hashes = [Sha256Hash(bytearray(unknown_tx_hash.binary()))
                             for unknown_tx_hash in tsk.unknown_tx_hashes()]
        unknown_tx_sids = tsk.unknown_tx_sids()
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )
        if tsk.success():
            btc_block_msg = BlockBtcMessage(buf=memoryview(tsk.block_message()))
            logger.debug(
                "Successfully parsed bx_block broadcast message. {0} transactions in bx_block".format(total_tx_count)
            )
        else:
            btc_block_msg = None
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in bx_block: {2}"
                        .format(len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count))
        block_info = get_block_info(
            bx_block,
            block_hash,
            tsk.short_ids(),
            total_tx_count,
            btc_block_msg
        )
        return btc_block_msg, block_info.block_hash, block_info.short_ids, unknown_tx_sids, unknown_tx_hashes

    def _create_compression_task(self) -> tpe.BTCBlockCompressionTask:  # pyre-ignore
        return tpe.BTCBlockCompressionTask(self._default_block_size)

    def _create_decompression_task(self) -> tpe.BTCBlockDecompressionTask:  # pyre-ignore
        return tpe.BTCBlockDecompressionTask(self._default_block_size)
