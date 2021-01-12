from typing import Set, TYPE_CHECKING, cast

from bxcommon import constants
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.feed.feed import Feed
from bxcommon.feed.feed_source import FeedSource

from bxgateway import gateway_constants

from bxgateway.feed.eth.eth_block_feed_entry import EthBlockFeedEntry
from bxgateway.feed.eth.eth_raw_block import EthRawBlock
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)

# Note: Block feed normal use case is only supported for gateways with a single blockchain connection
# i.e. Multi-node gateway users cannot assume the block has been accepted to their Ethereum instance


class EthNewBlockFeed(Feed[EthBlockFeedEntry, EthRawBlock]):
    NAME = "newBlocks"
    FIELDS = ["hash", "header", "transactions", "uncles"]
    ALL_FIELDS = FIELDS
    VALID_SOURCES = {
        FeedSource.BLOCKCHAIN_SOCKET, FeedSource.BLOCKCHAIN_RPC, FeedSource.BDN_SOCKET, FeedSource.BDN_INTERNAL
    }
    published_blocks: ExpiringSet[Sha256Hash]
    published_blocks_height: ExpiringSet[int]

    def __init__(self, node: "EthGatewayNode", network_num: int = constants.ALL_NETWORK_NUM,) -> None:
        super().__init__(self.NAME, network_num=network_num)
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
        block_queuing_service = cast(
            EthBlockQueuingService,
            self.node.block_queuing_service_manager.get_designated_block_queuing_service()
        )
        if block_queuing_service is None:
            return missing_blocks

        for block_number in range(start_block_height, end_block_height):
            block_hash = block_queuing_service.accepted_block_hash_at_height.contents.get(block_number)
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

        if block_number < self.last_block_number - gateway_constants.MAX_BLOCK_BACKLOG_TO_PUBLISH:
            # published block is too far behind ignore
            return

        if block_hash in self.published_blocks:
            # already published ignore
            return

        if raw_message.block is None:
            block_queuing_service = cast(
                EthBlockQueuingService,
                self.node.block_queuing_service_manager.get_designated_block_queuing_service()
            )
            best_accepted_height, _ = block_queuing_service.best_accepted_block

            logger.warning(
                "{} Feed Failed to recover block for message: {},"
                "last_block_published {} last block in queueing service {}",
                self.name, raw_message, self.last_block_number, best_accepted_height
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