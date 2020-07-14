import asyncio
from datetime import date

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon import constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.services.threaded_request_service import ThreadedRequestService
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxgateway.feed.new_transaction_feed import RawTransaction, RawTransactionFeedEntry
from bxgateway.rpc.provider.abstract_ws_provider import WsException
from bxgateway.rpc.ws.ws_server import WsServer
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_config_base import BdnServiceModelConfigBase


class WsProviderTest(AbstractTestCase):
    @async_test
    async def setUp(self) -> None:
        account_model = BdnAccountModelBase(
            "account_id", "account_name", "fake_certificate",
            new_transaction_streaming=BdnServiceModelConfigBase(
                expire_date=date(2999, 1, 1).isoformat()
            )
        )
        gateway_opts = gateway_helpers.get_gateway_opts(8000, ws=True)
        gateway_opts.set_account_options(account_model)

        self.gateway_node = MockGatewayNode(gateway_opts)
        self.gateway_node.requester = ThreadedRequestService(
            "mock_thread_service",
            self.gateway_node.alarm_queue,
            constants.THREADED_HTTP_POOL_SLEEP_INTERVAL_S
        )
        self.gateway_node.requester.start()
        self.server = WsServer(
            constants.LOCALHOST, 8005, self.gateway_node.feed_manager, self.gateway_node
        )
        self.ws_uri = f"ws://{constants.LOCALHOST}:8005"

        await self.server.start()

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
                memoryview(tx_contents)
            )
            serialized_published_message = RawTransactionFeedEntry(
                raw_published_message.tx_hash,
                raw_published_message.tx_contents
            )
            self.gateway_node.feed_manager.publish_to_feed(
                "newTxs", raw_published_message
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(
                subscription_id, subscription_message.subscription_id
            )
            self.assertEqual(
                serialized_published_message.tx_hash, subscription_message.notification["tx_hash"]
            )
            self.assertEqual(
                serialized_published_message.tx_contents, subscription_message.notification["tx_contents"]
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
    async def tearDown(self) -> None:
        await self.server.stop()
