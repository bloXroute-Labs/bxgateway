from typing import Dict, Any, List, Union

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import log_messages
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxcommon.messages.eth.serializers.block import Block
from bxutils import logging

logger = logging.get_logger(__name__)


class EthBlockFeedEntry:
    hash: str
    header: Dict[str, Any]
    transactions: List[Any]
    uncles: List[Any]

    def __init__(
        self, hash: Sha256Hash, block: Union[Block, InternalEthBlockInfo, memoryview]
    ) -> None:
        self.hash = f"0x{str(hash)}"
        try:
            if isinstance(block, Block):
                block_info = block
            elif isinstance(block, InternalEthBlockInfo):
                block_info = block.to_new_block_msg().get_block()
            else:
                block_info = InternalEthBlockInfo(block).to_new_block_msg().get_block()
            block_json = block_info.to_json()
            self.header = block_json["header"]
            self.transactions = block_json["transactions"]
            self.uncles = block_json["uncles"]
        except Exception as e:
            block_str = block
            if isinstance(block, memoryview):
                block_str = block.tobytes()

            logger.error(
                log_messages.COULD_NOT_DESERIALIZE_BLOCK,
                hash,
                block_str,
                e
            )
            raise e

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, EthBlockFeedEntry)
            and other.hash == self.hash
            and other.header == self.header
            and other.transactions == self.transactions
            and other.uncles == self.uncles
        )
