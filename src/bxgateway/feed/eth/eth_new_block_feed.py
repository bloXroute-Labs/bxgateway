from typing import Set, TYPE_CHECKING

from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import gateway_constants

from bxgateway.feed.feed import Feed
from bxgateway.feed.eth.eth_block_feed_entry import EthBlockFeedEntry
from bxgateway.feed.eth.eth_raw_block import EthRawBlock
from bxgateway.feed.feed_source import FeedSource

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

from bxutils import logging

MAX_BLOCK_BACKLOG_TO_PUBLISH = 10

logger = logging.get_logger(__name__)


class EthNewBlockFeed(Feed[EthBlockFeedEntry, EthRawBlock]):
    NAME = "newBlocks"
    FIELDS = ["hash", "header", "transactions", "uncles"]
    VALID_SOURCES = {
        FeedSource.BLOCKCHAIN_SOCKET, FeedSource.BLOCKCHAIN_RPC, FeedSource.BDN_SOCKET, FeedSource.BDN_INTERNAL
    }
    published_blocks: ExpiringSet[Sha256Hash]
    published_blocks_height: ExpiringSet[int]

    def __init__(self, node: "EthGatewayNode") -> None:
        super().__init__(self.NAME)
        self.last_block_number = 0
        self.hash_for_last_block_number = set()
        self.node = node
        self.published_blocks = ExpiringSet(
            node.alarm_queue, gateway_constants.MAX_BLOCK_CACHE_TIME_S, name="published_blocks"
        )
        self.published_blocks_height = ExpiringSet(
            node.alarm_queue, gateway_constants.MAX_BLOCK_CACHE_TIME_S, name="published_blocks_height"
        )

    def serialize(self, raw_message: EthRawBlock) -> EthBlockFeedEntry:
        block_message = raw_message.block
        assert block_message is not None
        return EthBlockFeedEntry(raw_message.block_hash, block_message)

    def publish_blocks_from_queue(self, start_block_height, end_block_height) -> Set[int]:
        missing_blocks = set()
        for block_number in range(start_block_height, end_block_height):
            block_hash = self.node.block_queuing_service.accepted_block_hash_at_height.contents.get(block_number)
            if block_hash:
                self.publish(
                    EthRawBlock(
                        block_number,
                        block_hash,
                        FeedSource.BDN_INTERNAL,
                        self.node._get_block_message_lazy(None, block_hash)
                    )
                )
            else:
                missing_blocks.add(block_number)
        return missing_blocks

    def publish(self, raw_message: EthRawBlock) -> None:
        logger.trace(
            "attempting to publish message: {} for feed {}", raw_message, self.name
        )
        if raw_message.source not in self.VALID_SOURCES:
            return
        if self.subscriber_count() == 0:
            return

        block_hash = raw_message.block_hash
        block_number = raw_message.block_number

        if block_number < self.last_block_number - MAX_BLOCK_BACKLOG_TO_PUBLISH:
            # published block is too far behind ignore
            return

        if block_hash in self.published_blocks:
            # already published ignore
            return

        if raw_message.block is None:
            logger.warning(
                "{} Feed Failed to recover block for message: {}",
                self.name, raw_message
            )
            return

        self.published_blocks.add(block_hash)
        self.published_blocks_height.add(block_number)

        if self.last_block_number and block_number > self.last_block_number + 1:
            # try to publish all intermediate blocks first
            missing_blocks = self.publish_blocks_from_queue(self.last_block_number + 1, block_number - 1)
            if missing_blocks:
                logger.info("Attempting to publish to feed block: {}, missing previous blocks {} ",
                            block_number, missing_blocks
                            )

        logger.debug("{} Processing new block message: {}", self.name, raw_message)
        super(EthNewBlockFeed, self).publish(raw_message)

        if block_number in self.published_blocks_height and block_number <= self.last_block_number:
            # possible fork, try to republish all later blocks
            _missing_blocks = self.publish_blocks_from_queue(block_number + 1, self.last_block_number)

        if block_number > self.last_block_number:
            self.last_block_number = block_number