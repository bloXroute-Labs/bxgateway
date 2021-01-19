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
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxutils import logging, utils

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)

RETRIES_MAX_ATTEMPTS = 8


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
        FeedSource.BLOCKCHAIN_SOCKET, FeedSource.BLOCKCHAIN_RPC, FeedSource.BDN_SOCKET
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
        self.blocks_confirmed_by_new_heads_notification = ExpiringSet(
            node.alarm_queue, gateway_constants.MAX_BLOCK_CACHE_TIME_S, name="receipts_feed_newHeads_confirmed_blocks"
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
        if raw_message.source not in self.VALID_SOURCES or self.subscriber_count() == 0:
            return
        if isinstance(raw_message, Dict):
            # transaction receipts published via parent publish method
            raise NotImplementedError

        block_hash = raw_message.block_hash
        block_number = raw_message.block_number
        block = raw_message.block

        # receipts won't be available until NewHeads feed notification
        if raw_message.source == FeedSource.BLOCKCHAIN_RPC:
            self.blocks_confirmed_by_new_heads_notification.add(block_hash)

        if block_number < self.last_block_number - gateway_constants.MAX_BLOCK_BACKLOG_TO_PUBLISH:
            # published block is too far behind, ignore
            return

        if block_hash in self.published_blocks:
            # already published ignore
            return

        if block is None:
            block = cast(InternalEthBlockInfo, self.node.block_queuing_service_manager.get_block_data(block_hash))
            if block is None:
                return

        assert block is not None

        if block_hash not in self.blocks_confirmed_by_new_heads_notification:
            return

        if raw_message.source == FeedSource.BDN_SOCKET:
            block = block.to_new_block_msg()

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
        block_hash_str = block_hash.to_string(True)
        for tx in block.txns():
            asyncio.create_task(self._publish(tx.hash().to_string(True), block_hash_str))

        if block_number in self.published_blocks_height and block_number <= self.last_block_number:
            # possible fork, try to republish all later blocks
            _missing_blocks = self._publish_blocks_from_queue(block_number + 1, self.last_block_number)

        if block_number > self.last_block_number:
            self.last_block_number = block_number

    async def _publish(
        self,
        transaction_hash: str,
        block_hash: str,
        retry_count: int = 0
    ) -> None:
        response = None
        try:
            response = await self.node.eth_ws_proxy_publisher.call_rpc(
                rpc_constants.ETH_GET_TRANSACTION_RECEIPT_RPC_METHOD, [transaction_hash],
            )
        except RpcError as e:
            error_response = e.to_json()
            logger.warning(
                "Failed to fetch transaction receipt for {} in block {}: {}. Ceasing attempts.",
                transaction_hash, block_hash, error_response
            )

        assert response is not None

        if response.result is None:
            if retry_count == 0 or retry_count == RETRIES_MAX_ATTEMPTS:
                logger.debug(
                    "Failed to fetch transaction receipt for tx {} in block {}: not found. "
                    "Attempt: {}. Max attempts: {}.",
                    transaction_hash, block_hash, retry_count + 1, RETRIES_MAX_ATTEMPTS + 1
                )
            if retry_count < RETRIES_MAX_ATTEMPTS:
                sleep_time = utils.fibonacci(retry_count + 1) * 0.1
                await asyncio.sleep(sleep_time)
                asyncio.create_task(self._publish(transaction_hash, block_hash, retry_count + 1))
            return

        response.result = humps.decamelize(response.result)
        json_response = response.to_json()
        if json_response["result"]["block_hash"] != block_hash:
            return
        super().publish(json_response)

        if retry_count > 0:
            logger.debug(
                "Succeeded in fetching receipt for tx {} in block {} after {} attempts.",
                transaction_hash, block_hash, retry_count
            )

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
                    block_hash_str = block_hash.to_string(True)
                    for tx in block.txns():
                        asyncio.create_task(self._publish(tx.hash().to_string(True), block_hash_str))
            else:
                missing_blocks.add(block_number)
        return missing_blocks
