import asyncio
import rlp
from datetime import date
from typing import Dict, Any

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon import constants
from bxcommon.feed.feed import FeedKey
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxcommon.models.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.rpc import rpc_constants
from bxcommon.services.threaded_request_service import ThreadedRequestService
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_config_base import BdnServiceModelConfigBase
from bxcommon.rpc.provider.abstract_ws_provider import WsException

from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.eth.eth_on_block_feed import EthOnBlockFeed, EventNotification
from bxcommon.feed.new_transaction_feed import (
    RawTransaction,
    RawTransactionFeedEntry,
    FeedSource,
)
from bxgateway.messages.eth.eth_normal_message_converter import (
    EthNormalMessageConverter,
)
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import (
    TransactionsEthProtocolMessage,
)
from bxgateway.rpc.ws.ws_server import WsServer
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.testing.mocks.mock_eth_ws_proxy_publisher import MockEthWsProxyPublisher
from bxutils import logging

logger = logging.get_logger(__name__)


def generate_new_eth_transaction() -> TxMessage:
    transaction = mock_eth_messages.get_dummy_transaction(1)
    transactions_eth_message = TransactionsEthProtocolMessage(None, [transaction])
    tx_message = EthNormalMessageConverter().tx_to_bx_txs(transactions_eth_message, 5)[
        0
    ][0]
    return tx_message


def generate_new_eth_with_to_transaction(to: str) -> TxMessage:
    transaction = mock_eth_messages.get_dummy_transaction(1, to_address_str=to)
    transactions_eth_message = TransactionsEthProtocolMessage(None, [transaction])
    tx_message = EthNormalMessageConverter().tx_to_bx_txs(transactions_eth_message, 5)[
        0
    ][0]
    return tx_message


def get_expected_eth_tx_contents(eth_tx_message: TxMessage) -> Dict[str, Any]:
    transaction = rlp.decode(eth_tx_message.tx_val().tobytes(), Transaction)
    expected_tx_contents = transaction.to_json()
    expected_tx_contents["gasPrice"] = expected_tx_contents["gas_price"]
    del expected_tx_contents["gas_price"]
    return expected_tx_contents


class WsProviderTest(AbstractTestCase):
    @async_test
    async def setUp(self) -> None:
        account_model = BdnAccountModelBase(
            "account_id",
            "account_name",
            "fake_certificate",
            new_transaction_streaming=BdnServiceModelConfigBase(
                expire_date=date(2999, 1, 1).isoformat()
            ),
        )
        gateway_opts = gateway_helpers.get_gateway_opts(8000, ws=True)
        gateway_opts.set_account_options(account_model)

        self.gateway_node = MockGatewayNode(gateway_opts)
        self.gateway_node.NODE_TYPE = NodeType.INTERNAL_GATEWAY
        self.transaction_streamer_peer = OutboundPeerModel(
            "127.0.0.1", 8006, node_type=NodeType.INTERNAL_GATEWAY
        )
        self.gateway_node.requester = ThreadedRequestService(
            "mock_thread_service",
            self.gateway_node.alarm_queue,
            constants.THREADED_HTTP_POOL_SLEEP_INTERVAL_S,
        )
        self.gateway_node.requester.start()
        self.server = WsServer(
            constants.LOCALHOST, 8005, self.gateway_node.feed_manager, self.gateway_node
        )
        self.ws_uri = f"ws://{constants.LOCALHOST}:8005"

        await self.server.start()

    @async_test
    async def test_eth_new_transactions_feed_default_subscribe(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(EthNewTransactionFeed())
        to = "0x1111111111111111111111111111111111111111"
        eth_tx_message = generate_new_eth_with_to_transaction(to[2:])
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(),
            eth_tx_message.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"
        logger.error(expected_tx_hash)
        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                "newTxs", options={"filters": f"to = {to} or to = aaaa"}
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(FeedKey("newTxs"), eth_transaction)

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

            expected_tx_contents = get_expected_eth_tx_contents(eth_tx_message)
            self.assertEqual(
                expected_tx_contents, subscription_message.notification["txContents"]
            )

    @async_test
    async def test_eth_new_tx_feed_subscribe_include_from_blockchain(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(EthNewTransactionFeed())

        eth_tx_message = generate_new_eth_transaction()
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(),
            eth_tx_message.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"

        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                "newTxs", {"include_from_blockchain": True}
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(FeedKey("newTxs"), eth_transaction)

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

            expected_tx_contents = get_expected_eth_tx_contents(eth_tx_message)
            self.assertEqual(
                expected_tx_contents, subscription_message.notification["txContents"]
            )

    @async_test
    async def test_eth_new_tx_feed_subscribe_not_include_from_blockchain(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(EthNewTransactionFeed())

        eth_tx_message = generate_new_eth_transaction()
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(), eth_tx_message.tx_val(), FeedSource.BDN_SOCKET
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"

        eth_tx_message_blockchain = generate_new_eth_transaction()
        eth_transaction_blockchain = EthRawTransaction(
            eth_tx_message_blockchain.tx_hash(),
            eth_tx_message_blockchain.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )

        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                "newTxs", {"include_from_blockchain": False}
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(FeedKey("newTxs"), eth_transaction)
            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("newTxs"), eth_transaction_blockchain
            )
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    ws.get_next_subscription_notification_by_id(subscription_id), 0.1
                )

    async def test_eth_pending_transactions_feed_default_subscribe(self):
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )

        eth_tx_message = generate_new_eth_transaction()
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(), eth_tx_message.tx_val(), FeedSource.BDN_SOCKET
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"

        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe("pendingTxs")

            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

    @async_test
    async def test_eth_pending_tx_feed_subscribe_handles_no_duplicates(self):
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )

        eth_tx_message = generate_new_eth_transaction()
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(), eth_tx_message.tx_val(), FeedSource.BDN_SOCKET
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"

        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe("pendingTxs", {"duplicates": False})

            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

            # will not publish twice
            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction
            )
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    ws.get_next_subscription_notification_by_id(subscription_id), 0.1
                )

    @async_test
    async def test_eth_pending_tx_feed_subscribe_with_duplicates(self):
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )

        eth_tx_message = generate_new_eth_transaction()
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(), eth_tx_message.tx_val(), FeedSource.BDN_SOCKET
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"

        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe("pendingTxs", {"duplicates": True})

            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

            # will publish twice
            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction
            )
            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

    @async_test
    async def test_onblock_feed_default_subscribe(self):
        block_height = 100
        name = "abc123"

        self.gateway_node.feed_manager.register_feed(EthOnBlockFeed(self.gateway_node))
        self.gateway_node.eth_ws_proxy_publisher = MockEthWsProxyPublisher(
            "", None, None, self.gateway_node
        )
        self.gateway_node.eth_ws_proxy_publisher.call_rpc = AsyncMock(
            return_value=JsonRpcResponse(request_id=1)
        )

        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                rpc_constants.ETH_ON_BLOCK_FEED_NAME,
                {"call_params": [{"data": "0x", "name": name}]},
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey(rpc_constants.ETH_ON_BLOCK_FEED_NAME),
                EventNotification(block_height=block_height),
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            print(subscription_message)
            self.assertEqual(
                block_height, subscription_message.notification["blockHeight"]
            )

            self.assertEqual(name, subscription_message.notification["name"])

    @async_test
    async def test_connection_and_close(self):
        async with WsProvider(self.ws_uri) as ws:
            self.assertTrue(ws.running)
            await self.server._connections[0].close()

    @async_test
    async def test_connection_and_close_unexpectedly(self):
        async with WsProvider(self.ws_uri) as ws:
            self.assertEqual(1, len(self.server._connections))

            connection_handler = self.server._connections[0]
            connection_handler.rpc_handler.handle_request = AsyncMock(
                side_effect=connection_handler.close
            )

            with self.assertRaises(WsException):
                await ws.subscribe("newTxs")

    @async_test
    async def test_connection_and_close_while_receiving_subscriptions(self):
        async with WsProvider(self.ws_uri) as ws:
            self.assertEqual(1, len(self.server._connections))

            connection_handler = self.server._connections[0]
            self.assertTrue(ws.running)
            subscription_id = await ws.subscribe("newTxs")

            tx_contents = helpers.generate_bytes(250)

            raw_published_message = RawTransaction(
                helpers.generate_object_hash(),
                memoryview(tx_contents),
                FeedSource.BDN_SOCKET,
            )
            serialized_published_message = RawTransactionFeedEntry(
                raw_published_message.tx_hash, raw_published_message.tx_contents
            )
            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("newTxs"), raw_published_message
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)

            self.assertEqual(
                serialized_published_message.tx_hash,
                subscription_message.notification["txHash"],
            )
            self.assertEqual(
                serialized_published_message.tx_contents,
                subscription_message.notification["txContents"],
            )

            task = asyncio.create_task(
                ws.get_next_subscription_notification_by_id(subscription_id)
            )
            await connection_handler.close()
            await asyncio.sleep(0.01)

            exception = task.exception()
            self.assertIsInstance(exception, WsException)

    @async_test
    async def test_connection_to_invalid_channel(self):
        async with WsProvider(self.ws_uri) as ws:
            self.assertEqual(1, len(self.server._connections))

            connection_handler = self.server._connections[0]
            with self.assertRaises(RpcError):
                _ = await ws.subscribe("fake_channel")
            await connection_handler.close()

    @async_test
    async def test_multiple_rpc_calls_mixed_response(self):
        # unlikely to ever actually happen in practice, but should be handled
        async with WsProvider(self.ws_uri) as ws:
            self.assertEqual(1, len(self.server._connections))

            connection_handler = self.server._connections[0]
            connection_handler.rpc_handler.handle_request = AsyncMock()

            subscribe_task_1 = asyncio.create_task(
                ws.call_bx(RpcRequestType.SUBSCRIBE, ["newTxs"], request_id="123")
            )
            await asyncio.sleep(0)
            subscribe_task_2 = asyncio.create_task(
                ws.call_bx(RpcRequestType.SUBSCRIBE, ["newTxs"], request_id="124")
            )

            server_ws = connection_handler.ws
            assert server_ws is not None

            # send responses out of order
            await server_ws.send(JsonRpcResponse("124", "subid2").to_jsons())
            await server_ws.send(JsonRpcResponse("123", "subid1").to_jsons())

            await asyncio.sleep(0.01)
            self.assertTrue(subscribe_task_1.done())
            self.assertTrue(subscribe_task_2.done())

            subscription_1 = subscribe_task_1.result()
            self.assertEqual("123", subscription_1.id)
            self.assertEqual("subid1", subscription_1.result)

            subscription_2 = subscribe_task_2.result()
            self.assertEqual("subid2", subscription_2.result)

    @async_test
    async def test_eth_new_transactions_feed_subscribe_filters(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(EthNewTransactionFeed())
        to = "0x1111111111111111111111111111111111111111"
        eth_tx_message = generate_new_eth_with_to_transaction(to[2:])
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(),
            eth_tx_message.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"
        logger.error(expected_tx_hash)
        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                "newTxs", options={"filters": f"to = {to} or to = aaaa"}
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(FeedKey("newTxs"), eth_transaction)

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash, subscription_message.notification["txHash"]
            )

            expected_tx_contents = get_expected_eth_tx_contents(eth_tx_message)
            self.assertEqual(
                expected_tx_contents, subscription_message.notification["txContents"]
            )

    @async_test
    async def test_eth_new_transactions_feed_subscribe_filters2(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(EthNewTransactionFeed())
        to = "0x1111111111111111111111111111111111111112"
        eth_tx_message = generate_new_eth_with_to_transaction(to[2:])
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(),
            eth_tx_message.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"
        to2 = "0x1111111111111111111111111111111111111111"
        eth_tx_message2 = generate_new_eth_with_to_transaction(to2[2:])
        eth_transaction2 = EthRawTransaction(
            eth_tx_message2.tx_hash(),
            eth_tx_message2.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash2 = f"0x{str(eth_transaction2.tx_hash)}"
        logger.error(expected_tx_hash2)
        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                "newTxs",
                options={
                    "filters": f"to = 0x1111111111111111111111111111111111111111 or to = aaaa"
                },
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(FeedKey("newTxs"), eth_transaction)
            self.gateway_node.feed_manager.publish_to_feed_by_key(FeedKey("newTxs"), eth_transaction2)

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash2, subscription_message.notification["txHash"]
            )

            expected_tx_contents = get_expected_eth_tx_contents(eth_tx_message2)
            self.assertEqual(
                expected_tx_contents, subscription_message.notification["txContents"]
            )

    @async_test
    async def test_eth_pending_transactions_feed_subscribe_filters3(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )
        to = "0x1111111111111111111111111111111111111112"
        eth_tx_message = generate_new_eth_with_to_transaction(to[2:])
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(),
            eth_tx_message.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash = f"0x{str(eth_transaction.tx_hash)}"
        to2 = "0x1111111111111111111111111111111111111111"
        eth_tx_message2 = generate_new_eth_with_to_transaction(to2[2:])
        eth_transaction2 = EthRawTransaction(
            eth_tx_message2.tx_hash(),
            eth_tx_message2.tx_val(),
            FeedSource.BLOCKCHAIN_SOCKET,
        )
        expected_tx_hash2 = f"0x{str(eth_transaction2.tx_hash)}"
        logger.error(expected_tx_hash2)
        async with WsProvider(self.ws_uri) as ws:
            subscription_id = await ws.subscribe(
                "pendingTxs",
                options={
                    "filters": f"to in [0x1111111111111111111111111111111111111111, aaaa]"
                },
            )

            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction
            )
            self.gateway_node.feed_manager.publish_to_feed_by_key(
                FeedKey("pendingTxs"), eth_transaction2
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(subscription_id, subscription_message.subscription_id)
            self.assertEqual(
                expected_tx_hash2, subscription_message.notification["txHash"]
            )

            expected_tx_contents = get_expected_eth_tx_contents(eth_tx_message2)
            self.assertEqual(
                expected_tx_contents, subscription_message.notification["txContents"]
            )

    @async_test
    async def tearDown(self) -> None:
        await self.server.stop()
