from typing import NamedTuple

from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.feed import Feed


class RawBlockFeedEntry:
    hash: str
    block: str

    def __init__(
        self,
        hash: Sha256Hash,
        block: memoryview
    ) -> None:
        self.hash = str(hash)
        self.block = convert.bytes_to_hex(block)

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, RawBlockFeedEntry)
            and other.hash == self.hash
            and other.block == self.block
        )


class RawBlock(NamedTuple):
    hash: Sha256Hash
    block: memoryview


class NewBlockFeed(Feed[RawBlockFeedEntry, RawBlock]):
    NAME = "newBlocks"
    FIELDS = ["hash", "block"]

    def __init__(self) -> None:
        super().__init__(self.NAME)

    def serialize(self, raw_message: RawBlock) -> RawBlockFeedEntry:
        return RawBlockFeedEntry(raw_message.hash, raw_message.block)
