import asyncio
from typing import TYPE_CHECKING, Dict, cast, Set, Union

import humps

from bxcommon import constants
from bxcommon.feed.feed import Feed
from bxcommon.feed.feed_source import FeedSource
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.utils.expiring_set import ExpiringSet
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants
from bxgateway.feed.eth.eth_raw_block import EthRawBlock
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxutils import logging

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)


class TransactionReceiptsFeedEntry:
    receipt: Dict[str, str]

    def __init__(self, receipt: Dict):
        self.receipt = receipt

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, TransactionReceiptsFeedEntry)
            and other.receipt == self.receipt
        )


class EthTransactionReceiptsFeed(Feed[TransactionReceiptsFeedEntry, Union[EthRawBlock, Dict]]):
    NAME = rpc_constants.ETH_TRANSACTION_RECEIPTS_FEED_NAME
    FIELDS = [
        "receipt",
        "receipt.block_hash",
        "receipt.block_number",
        "receipt.contract_address",
        "receipt.cumulative_gas_used",
        "receipt.from",
        "receipt.gas_used",
        "receipt.logs",
        "receipt.logs_bloom",
        "receipt.status",
        "receipt.to",
        "receipt.transaction_hash",
        "receipt.transaction_index"
    ]
    ALL_FIELDS = ["receipt"]
    VALID_SOURCES = {
        FeedSource.BLOCKCHAIN_SOCKET, FeedSource.BLOCKCHAIN_RPC
    }

    def __init__(self, node: "EthGatewayNode", network_num: int = constants.ALL_NETWORK_NUM, ) -> None:
        super().__init__(self.NAME, network_num)
        self.node = node
        self.last_block_number = 0
        self.published_blocks = ExpiringSet(
            node.alarm_queue, gateway_constants.MAX_BLOCK_CACHE_TIME_S, name="receipts_feed_published_blocks"
        )
        self.published_blocks_height = ExpiringSet(
            node.alarm_queue, gateway_constants.MAX_BLOCK_CACHE_TIME_S, name="receipts_feed_published_blocks_height"
        )

    def serialize(self, raw_message: Union[EthRawBlock, Dict]) -> TransactionReceiptsFeedEntry:
        # only receipts are serialized for publishing
        if isinstance(raw_message, Dict):
            return TransactionReceiptsFeedEntry(raw_message["result"])
        else:
            raise NotImplementedError

    def publish(self, raw_message: Union[EthRawBlock, Dict]) -> None:
        logger.trace(
            "attempting to publish message: {} for feed {}", raw_message, self.name
        )
        if isinstance(raw_message, Dict):
            # transaction receipts published via parent publish method
            raise NotImplementedError
        if raw_message.source not in self.VALID_SOURCES or raw_message.block is None or self.subscriber_count() == 0:
            return

        block_hash = raw_message.block_hash
        block_number = raw_message.block_number
        block = raw_message.block
        assert block is not None

        if block_number < self.last_block_number - gateway_constants.MAX_BLOCK_BACKLOG_TO_PUBLISH:
            # published block is too far behind, ignore
            return

        if block_hash in self.published_blocks:
            # already published ignore
            return

        self.published_blocks.add(block_hash)
        self.published_blocks_height.add(block_number)

        if self.last_block_number and block_number > self.last_block_number + 1:
            # try to publish all intermediate blocks first
            missing_blocks = self._publish_blocks_from_queue(self.last_block_number + 1, block_number - 1)
            if missing_blocks:
                logger.info(
                    "Attempting to publish to feed block: {}, missing previous blocks {} ", block_number, missing_blocks
                )

        logger.debug("{} Attempting to fetch transaction receipts for block {}", self.name, block_hash)
        for tx in block.txns():
            asyncio.create_task(self._publish(tx.hash()))

        if block_number in self.published_blocks_height and block_number <= self.last_block_number:
            # possible fork, try to republish all later blocks
            _missing_blocks = self._publish_blocks_from_queue(block_number + 1, self.last_block_number)

        if block_number > self.last_block_number:
            self.last_block_number = block_number

    async def _publish(
        self,
        transaction_hash: Sha256Hash
    ) -> None:
        response = None
        try:
            response = await self.node.eth_ws_proxy_publisher.call_rpc(
                rpc_constants.ETH_GET_TRANSACTION_RECEIPT_RPC_METHOD, [transaction_hash],
            )
        except RpcError as e:
            error_response = e.to_json()
            logger.debug("Failed to fetch transaction receipt for {}: {}", transaction_hash, error_response)
        if not response:
            logger.debug("Failed to fetch transaction receipt for {}: transaction was not found.", transaction_hash)
            return

        response.result = humps.decamelize(response.result)
        super().publish(response.to_json())

    def _publish_blocks_from_queue(self, start_block_height, end_block_height) -> Set[int]:
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
                block = self.node.block_queuing_service_manager.get_block_data(block_hash)
                if block is not None:
                    for tx in block.txns():
                        asyncio.create_task(self._publish(tx.hash()))
            else:
                missing_blocks.add(block_number)
        return missing_blocks
