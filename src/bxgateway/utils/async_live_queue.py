import asyncio
from typing import TypeVar, Generic

T = TypeVar("T")


class AsyncLiveQueue(asyncio.Queue, Generic[T]):
    """
    asyncio.Queue with liveliness characteristics.

    When the queue is full, old items are ejected in favor of newer entries.
    """

    def __init__(self, max_size: int):
        super().__init__(max_size)

    async def put(self, item: T) -> None:
        if self.full():
            self.get_nowait()
        return self.put_nowait(item)
