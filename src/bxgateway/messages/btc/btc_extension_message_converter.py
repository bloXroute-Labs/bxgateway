import datetime
from collections import deque, namedtuple
import time
from typing import Tuple

from bxcommon.utils import convert

from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash

import task_pool_executor as tpe  # pyre-ignore for now, figure this out later (stub file or Python wrapper?)

CompressionTaskData = namedtuple("CompressionTaskData", ["task", "return_array"])


# TODO : convert to async
def wait_for_task(tsk):
    while not tsk.is_completed():
        time.sleep(0)
        continue


class BtcExtensionMessageConverter(AbstractBtcMessageConverter):

    QUEUE_GROW_SIZE = 10

    def __init__(self, btc_magic):
        super(BtcExtensionMessageConverter, self).__init__(btc_magic)
        self.compression_tasks = deque()

    def block_to_bx_block(self, block_msg, tx_service) -> Tuple[memoryview, BlockInfo]:
        compress_start_datetime = datetime.datetime.utcnow()
        compress_start_timestamp = time.time()
        try:
            tsk = self.compression_tasks.pop()
        except IndexError:
            tsk = None
            for i in range(self.QUEUE_GROW_SIZE):
                tsk = tpe.BTCBlockCompressionTask(len(block_msg.buf))
                if i < self.QUEUE_GROW_SIZE - 1:
                    self.compression_tasks.append(tsk)
        assert tsk is not None
        tsk.init(tpe.InputBytes(block_msg.buf), tx_service.proxy)
        tpe.enqueue_task(tsk)
        wait_for_task(tsk)
        bx_block = tsk.bx_block()
        block = memoryview(bx_block)

        compressed_size = len(block)
        original_size = len(block_msg.rawbytes())
        block_hash = BtcObjectHash(
            binary=convert.hex_to_bytes(tsk.block_hash().hex_string())
        )

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
        self.compression_tasks.append(tsk)
        return block, block_info
