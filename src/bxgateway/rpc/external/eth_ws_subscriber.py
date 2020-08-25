import asyncio
from asyncio import Future
from typing import Optional, cast, List, Tuple, Dict, Any, TYPE_CHECKING

from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import log_messages
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxgateway.feed.new_transaction_feed import FeedSource
from bxgateway.rpc.provider.abstract_ws_provider import AbstractWsProvider, WsException
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)

SUBSCRIBE_REQUEST_ID = "1"


class EthWsSubscriber(AbstractWsProvider):
    """
    Subscriber to Ethereum websockets RPC interface. Publishes transactions
    accepted to Ethereum mempool to a `pendingTxs` feed.

    Requires Ethereum startup arguments:
    --ws --wsapi eth --wsport 8546

    See https://geth.ethereum.org/docs/rpc/server for more info.
    (--ws-addr 127.0.0.1 maybe a good idea too)

    Requires Gateway startup arguments:
    --eth-ws-uri ws://127.0.0.1:8546
    """

    def __init__(
        self,
        ws_uri: Optional[str],
        feed_manager: FeedManager,
        transaction_service: TransactionService,
        node: "EthGatewayNode"
    ) -> None:
        self.feed_manager = feed_manager
        self.transaction_service = transaction_service
        self.node = node

        # ok, lifecycle patterns are a bit different
        # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
        super().__init__(ws_uri, True)
        self.receiving_tasks: List[Future] = []

    async def revive(self) -> None:
        """
        Revives subscriber; presumably, subscriber got disconnected earlier
        and stopped retrying.
        """
        if self.ws is None and not self.running:
            logger.info("Attempting to revive Ethereum websockets feed...")
            await self.reconnect()

    async def reconnect(self) -> None:
        logger.warning(log_messages.ETH_WS_SUBSCRIBER_CONNECTION_BROKEN)

        for receiving_task in self.receiving_tasks:
            receiving_task.cancel()
        self.receiving_tasks = []

        try:
            await super().reconnect()
        except ConnectionRefusedError:
            self.running = False

        if self.running:
            await self.subscribe_to_feeds()
            logger.info("Reconnected to Ethereum websocket feed")
        else:
            logger.warning(log_messages.ETH_RPC_COULD_NOT_RECONNECT)

    async def subscribe(self, channel: str, options: Optional[Dict[str, Any]] = None) -> str:
        response = await self.call_rpc(
            "eth_subscribe",
            [channel]
        )
        subscription_id = response.result
        assert isinstance(subscription_id, str)
        self.subscription_manager.register_subscription(subscription_id)
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        response = await self.call_rpc(
            "eth_unsubscribe",
            [subscription_id]
        )
        if response.result is not None:
            self.subscription_manager.unregister_subscription(subscription_id)
            return True, None
        else:
            return False, response.error

    async def start(self) -> None:
        ws_uri = self.uri
        if ws_uri is not None:

            await self.initialize()

            logger.info("Subscribed to Ethereum websocket feed.")
            await self.subscribe_to_feeds()

    async def subscribe_to_feeds(self):
        subscription_id = await self.subscribe("newPendingTransactions")
        self.receiving_tasks.append(asyncio.create_task(self.handle_tx_notifications(subscription_id)))

        subscription_id = await self.subscribe("newHeads")
        self.receiving_tasks.append(asyncio.create_task(self.handle_block_notifications(subscription_id)))

    async def handle_tx_notifications(self, subscription_id: str) -> None:
        while self.running:
            next_notification = await self.get_next_subscription_notification_by_id(
                subscription_id
            )
            transaction_hash = next_notification.notification
            assert isinstance(transaction_hash, str)
            self.process_received_transaction(
                Sha256Hash(
                    convert.hex_to_bytes(
                        transaction_hash[2:]
                    )
                )
            )

    async def handle_block_notifications(self, subscription_id: str) -> None:
        while self.running:
            next_notification = await self.get_next_subscription_notification_by_id(
                subscription_id
            )
            logger.debug(
                "NewBlockHeader Notification {} from node", next_notification
            )
            block_header = next_notification.notification
            block_hash = Sha256Hash(convert.hex_to_bytes(block_header["hash"][2:]))
            block_number = int(block_header["number"], 16)

            self.node.publish_block(
                block_number, block_hash, None, FeedSource.BLOCKCHAIN_RPC
            )

    def process_received_transaction(self, tx_hash: Sha256Hash) -> None:
        tx_contents = cast(
            Optional[memoryview],
            self.transaction_service.get_transaction_by_hash(tx_hash)
        )
        if tx_contents is None:
            asyncio.create_task(self.fetch_missing_transaction(tx_hash))
        else:
            self.process_transaction_with_contents(tx_hash, tx_contents)

    def process_transaction_with_contents(
        self, tx_hash: Sha256Hash, tx_contents: memoryview
    ) -> None:
        transaction_feed_stats_service.log_pending_transaction_from_local(tx_hash)

        self.feed_manager.publish_to_feed(
            EthPendingTransactionFeed.NAME,
            EthRawTransaction(tx_hash, tx_contents, FeedSource.BLOCKCHAIN_RPC)
        )

    async def fetch_missing_transaction(self, tx_hash: Sha256Hash) -> None:
        try:
            response = await self.call_rpc(
                "eth_getTransactionByHash",
                [f"0x{str(tx_hash)}"]
            )
            self.process_transaction_with_parsed_contents(tx_hash, response.result)
        except WsException:
            # ok, don't continue processing
            logger.debug(
                "Attempt to fetch transaction {} was interrupted by a broken connection. "
                "Abandoning.",
                tx_hash
            )
            pass

    def process_transaction_with_parsed_contents(
        self, tx_hash: Sha256Hash, parsed_tx: Optional[Dict[str, Any]]
    ) -> None:
        transaction_feed_stats_service.log_pending_transaction_from_local(tx_hash)

        if parsed_tx is None:
            logger.debug(log_messages.TRANSACTION_NOT_FOUND_IN_MEMPOOL, tx_hash)
            transaction_feed_stats_service.log_pending_transaction_missing_contents()
        else:
            self.feed_manager.publish_to_feed(
                EthPendingTransactionFeed.NAME,
                EthRawTransaction(tx_hash, parsed_tx, FeedSource.BLOCKCHAIN_RPC)
            )

    async def stop(self) -> None:
        await self.close()
        for receiving_task in self.receiving_tasks:
            receiving_task.cancel()
