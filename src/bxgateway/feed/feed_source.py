from enum import Enum, auto


class FeedSource(Enum):
    BLOCKCHAIN_SOCKET = auto()
    BLOCKCHAIN_RPC = auto()
    BDN_SOCKET = auto()
    BDN_INTERNAL = auto()
