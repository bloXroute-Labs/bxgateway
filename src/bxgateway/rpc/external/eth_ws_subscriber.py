import asyncio
from asyncio import Future
from typing import Optional, cast

# noinspection PyPackageRequirements
import websockets

from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.feed.pending_transaction_feed import PendingTransactionFeed
from bxgateway.feed.unconfirmed_transaction_feed import TransactionFeedEntry
from bxutils import logging

logger = logging.get_logger(__name__)

SUBSCRIBE_REQUEST_ID = "1"


class EthWsSubscriber:
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
        self.ws_uri = ws_uri
        self.feed_manager = feed_manager
        self.transaction_service = transaction_service

        self.ws_client: Optional[websockets.WebSocketClientProtocol] = None
        self.subscription_id: Optional[str] = None
        self.receiving_task: Optional[Future] = None

    async def start(self) -> None:
        ws_uri = self.ws_uri
        if ws_uri is not None:
            ws_client = await websockets.connect(ws_uri)
            logger.debug("Connected to Ethereum websockets RPC interface at: {}", ws_uri)
            self.ws_client = ws_client

            await ws_client.send(
                JsonRpcRequest(
                    SUBSCRIBE_REQUEST_ID,
                    "eth_subscribe",
                    ["newPendingTransactions"]
                ).to_jsons()
            )

            self.receiving_task = asyncio.create_task(self.receive_updates())

    async def receive_updates(self) -> None:
        while True:
            ws_client = self.ws_client
            assert ws_client is not None

            rpc_message_str = await ws_client.recv()

            if self.subscription_id is None:
                rpc_response = JsonRpcResponse.from_jsons(rpc_message_str)
                if (
                    SUBSCRIBE_REQUEST_ID != rpc_response.id
                    or rpc_response.error is not None
                ):
                    logger.error(
                        "Could not initialize RPC subscriber: {}",
                        rpc_response
                    )
                    break
                self.subscription_id = rpc_response.result
                logger.debug(
                    "Initialized pending transaction feed with subscription: {}",
                    self.subscription_id
                )
            else:
                subscription_message = JsonRpcRequest.from_jsons(rpc_message_str)
                params = subscription_message.params
                assert isinstance(params, dict)
                assert params["subscription"] == self.subscription_id
                tx_hash = Sha256Hash(
                    convert.hex_to_bytes(
                        params["result"][2:]
                    )
                )
                self.process_received_transaction(tx_hash)

    def process_received_transaction(self, tx_hash: Sha256Hash) -> None:
        tx_contents = cast(
            Optional[memoryview],
            self.transaction_service.get_transaction_by_hash(tx_hash)
        )
        self.feed_manager.publish_to_feed(
            PendingTransactionFeed.NAME,
            TransactionFeedEntry(tx_hash, tx_contents)
        )

    async def stop(self) -> None:
        receiving_task = self.receiving_task
        if receiving_task is not None:
            receiving_task.cancel()

        ws_client = self.ws_client
        if ws_client is not None:
            ws_client.close()
