import datetime
from typing import NamedTuple, Optional, List

from bxcommon.utils.object_hash import Sha256Hash


class BlockInfo(NamedTuple):
    block_hash: Sha256Hash
    short_ids: List[int]
    start_datetime: datetime.datetime
    end_datetime: datetime.datetime
    duration_ms: float
    txn_count: Optional[int]
    compressed_block_hash: Optional[str]
    prev_block_hash: Optional[str]
    original_size: Optional[float]
    compressed_size: Optional[float]
    compression_rate: Optional[float]
    ignored_short_ids: List[Optional[int]]
