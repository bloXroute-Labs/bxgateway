from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.utils.async_live_queue import AsyncLiveQueue


class AsyncLiveQueueTest(AbstractTestCase):

    @async_test
    async def test_queue_put_remove(self):
        queue: AsyncLiveQueue[int] = AsyncLiveQueue(10)

        await queue.put(1)
        await queue.put(2)
        await queue.put(3)

        self.assertEqual(1, await queue.get())
        self.assertEqual(2, await queue.get())
        self.assertEqual(3, await queue.get())

    @async_test
    async def test_queue_put_remove_full(self):
        queue: AsyncLiveQueue[int] = AsyncLiveQueue(2)

        await queue.put(1)
        await queue.put(2)
        await queue.put(3)

        self.assertEqual(2, await queue.get())
        self.assertEqual(3, await queue.get())
