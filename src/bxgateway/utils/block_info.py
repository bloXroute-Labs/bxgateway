from collections import namedtuple

BlockInfo = namedtuple(
    "BlockInfo",
    ["txn_count", "block_hash", "compressed_block_hash", "prev_block_hash", "short_ids"]
)
