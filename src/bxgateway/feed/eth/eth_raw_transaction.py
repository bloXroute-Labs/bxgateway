from typing import NamedTuple, Union, Dict, Any

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.new_transaction_feed import FeedSource


class EthRawTransaction(NamedTuple):
    """
    Transaction contents can be any of the following:
     - confirmed transaction from transaction streaming gateway (memoryview)
     - transaction contents from BDN transaction service (memoryview)
     - transaction from Ethereum node socket connection (memoryview)
     - transaction from Ethereum RPC call (JSON)

    Raw pre-serialized transaction must be able to handle all cases.
    """
    tx_hash: Sha256Hash
    tx_contents: Union[memoryview, Dict[str, Any]]
    source: FeedSource
