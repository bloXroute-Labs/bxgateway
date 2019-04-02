from collections import deque, namedtuple
import time

from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter
import task_pool_executor as tpe

from bxgateway.utils.block_info import BlockInfo

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

    def block_to_bx_block(self, btc_block_msg, tx_service):
        try:
            tsk = self.compression_tasks.pop()
        except IndexError:
            tsk = None
            for i in range(self.QUEUE_GROW_SIZE):
                tsk = tpe.BTCBlockCompressionTask(len(btc_block_msg.buf))
                if i < self.QUEUE_GROW_SIZE - 1:
                    self.compression_tasks.append(tsk)
        tsk.init(tpe.InputBytes(btc_block_msg.buf), tx_service.cpp_tx_hash_to_short_ids)
        tpe.enqueue_task(tsk)
        wait_for_task(tsk)
        bx_block = tsk.bx_block()
        block = memoryview(bx_block)
        block_info = BlockInfo(
            tsk.txn_count(),
            tsk.block_hash().hex_string(),
            tsk.compressed_block_hash().hex_string(),
            tsk.prev_block_hash().hex_string(),
            tsk.short_ids()
        )
        self.compression_tasks.append(tsk)
        return block, block_info
