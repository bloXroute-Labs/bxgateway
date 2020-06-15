import asyncio
from asyncio import Future
from typing import Optional, cast, List, Tuple, Dict, Any

import rlp

from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import log_messages
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.feed.pending_transaction_feed import PendingTransactionFeed
from bxgateway.feed.new_transaction_feed import TransactionFeedEntry
from bxgateway.messages.eth.serializers.transaction import Transaction
from bxgateway.rpc.provider.abstract_ws_provider import AbstractWsProvider
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service
from bxutils import logging

logger = logging.get_logger(__name__)

SUBSCRIBE_REQUEST_ID = "1"


class EthWsSubscriber(AbstractWsProvider):
    """
    Subscriber to Ethereum websockets RPC interface. Publishes transactions
    accepted to Ethereum mempool to a `pendingTxs` feed.

    Requires Ethereum startup arguments:
    --ws --wsapi eth --wsport 8546

    See https://geth.ethereum.org/docs/rpc/server for more info.

    Requires Gateway startup arguments:
    --eth-ws-uri ws://127.0.0.1:8546
    """

    def __init__(
        self,
        ws_uri: Optional[str],
        feed_manager: FeedManager,
        transaction_service: TransactionService
    ) -> None:
        self.feed_manager = feed_manager
        self.transaction_service = transaction_service

        # ok, lifecycle patterns are a bit different
        # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
        super().__init__(ws_uri)
        self.receiving_task: Optional[Future] = None

    async def subscribe(self, channel: str, fields: Optional[List[str]] = None) -> str:
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

            subscription_id = await self.subscribe("newPendingTransactions")
            self.receiving_task = asyncio.create_task(self.handle_notifications(subscription_id))

    async def handle_notifications(self, subscription_id: str) -> None:
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
        try:
            transaction = rlp.decode(tx_contents.tobytes(), Transaction)
            self.feed_manager.publish_to_feed(
                PendingTransactionFeed.NAME,
                TransactionFeedEntry(tx_hash, transaction.to_json())
            )
            transaction_feed_stats_service.log_pending_transaction_from_local(tx_hash)

        except Exception as e:
            logger.error(
                log_messages.COULD_NOT_DESERIALIZE_TRANSACTION, tx_hash, e, exc_info=True
            )

    async def fetch_missing_transaction(self, tx_hash: Sha256Hash) -> None:
        response = await self.call_rpc(
            "eth_getTransactionByHash",
            [f"0x{str(tx_hash)}"]
        )
        return self.process_transaction_with_parsed_contents(tx_hash, response.result)

    def process_transaction_with_parsed_contents(
        self, tx_hash: Sha256Hash, parsed_tx: Optional[Dict[str, Any]]
    ) -> None:
        if parsed_tx is None:
            logger.debug(log_messages.TRANSACTION_NOT_FOUND_IN_MEMPOOL, tx_hash)
            transaction_feed_stats_service.log_pending_transaction_missing_contents()
        else:
            # normalize transaction format
            parsed_tx = Transaction.from_json(parsed_tx).to_json()

        self.feed_manager.publish_to_feed(
            PendingTransactionFeed.NAME,
            TransactionFeedEntry(tx_hash, parsed_tx)
        )
        transaction_feed_stats_service.log_pending_transaction_from_local(tx_hash)

    async def stop(self) -> None:
        await self.close()

        receiving_task = self.receiving_task
        if receiving_task is not None:
            receiving_task.cancel()
