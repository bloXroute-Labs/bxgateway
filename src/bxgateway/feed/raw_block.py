from typing import Iterator, Optional, TypeVar, Generic
from dataclasses import dataclass

from bxgateway.feed.feed_source import FeedSource
from bxcommon.utils.object_hash import Sha256Hash

T = TypeVar("T")


@dataclass
class AbstractRawBlock(Generic[T]):
    """
    AbstractBlock Notification Object

    """
    block_number: int
    block_hash: Sha256Hash
    source: FeedSource
    delayed_block: Iterator[T]
    _block: Optional[T] = None

    @property
    def block(self) -> Optional[T]:
        if self._block is None:
            try:
                self._block = next(self.delayed_block)
            except StopIteration:
                pass
        return self._block
