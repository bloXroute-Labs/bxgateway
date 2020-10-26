from typing import Optional

from bxcommon.utils.alarm_queue import AlarmQueue
from bxutils import logging

logger = logging.get_logger(__name__)


class IntervalMinimum:

    def __init__(self, interval_len_s: int, alarm_queue: AlarmQueue):
        self.current_minimum: int = 0

        self._interval_len_s = interval_len_s
        self._next_interval_minimum: Optional[int] = None
        alarm_queue.register_alarm(self._interval_len_s, self._on_interval_end)

    def add(self, new_value: int):
        next_interval_minimum = self._next_interval_minimum

        if next_interval_minimum is None or new_value < next_interval_minimum:
            self._next_interval_minimum = new_value

    def _on_interval_end(self):
        if self._next_interval_minimum is None:
            self.current_minimum = 0
        else:
            self.current_minimum = self._next_interval_minimum
        self._next_interval_minimum = None

        return self._interval_len_s
