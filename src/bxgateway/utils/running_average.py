from collections import deque
from typing import List, Deque


class RunningAverage:

    def __init__(self, max_count: int) -> None:
        self.max_count: int = max_count
        self.average: float = 0
        self.values: Deque[float] = deque()

        self.max_next_multiplier = 1 / self.max_count

    def add_value(self, value: float) -> None:
        self.values.append(value)
        if len(self.values) <= self.max_count:
            previous_multiplier = (len(self.values) - 1) / len(self.values)
            next_multiplier = 1 / len(self.values)
            self.average = self.average * previous_multiplier + value * next_multiplier
        else:
            oldest_item = self.values.popleft()
            self.average -= oldest_item * self.max_next_multiplier
            self.average += value * self.max_next_multiplier
