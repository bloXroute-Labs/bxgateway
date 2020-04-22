import time
from collections import deque
from typing import Deque, Optional

from bxcommon.messages.abstract_message import AbstractMessage


class BlockchainMessageQueue:
    """
    Message queue with defined TTL from first object insertion.

    The intention of this queue is to forward queued up messages only if the queue is emptied within the TTL.
    (e.g. if the blockchain connection doesn't recovery fast enough the messages are dropped)
    """
    _queue: Deque[AbstractMessage]
    _time_to_live: int
    _first_item_insertion_time: Optional[float]

    def __init__(self, time_to_live: int):
        self._time_to_live = time_to_live
        self._initialize_queue()

    def append(self, message: AbstractMessage):
        if self._first_item_insertion_time is None:
            self._first_item_insertion_time = time.time()

        if not self._has_queue_expired():
            self._queue.append(message)

    def pop_items(self) -> Deque[AbstractMessage]:
        self._clear_if_needed()
        return_value = self._queue
        self._initialize_queue()
        return return_value

    def _initialize_queue(self):
        self._queue = deque()
        self._first_item_insertion_time = None

    def _clear_if_needed(self):
        if self._has_queue_expired():
            self._queue.clear()

    def _has_queue_expired(self) -> bool:
        first_item_insertion_time = self._first_item_insertion_time
        return (
            first_item_insertion_time is not None and
            time.time() - first_item_insertion_time > self._time_to_live
        )
