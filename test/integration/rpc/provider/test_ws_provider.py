import asyncio

from bloxroute_cli.provider.ws_provider import WsProvider, WsException
from bxcommon import constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.services.threaded_request_service import ThreadedRequestService
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxgateway.feed.new_transaction_feed import TransactionFeedEntry
from bxgateway.rpc.ws.ws_server import WsServer
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class WsProviderTest(AbstractTestCase):
    @async_test
    async def setUp(self) -> None:
        self.gateway_node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000)
        )
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
            subscription_id = await ws.subscribe("newTxs")

            tx_contents = helpers.generate_bytes(250)

            published_message = TransactionFeedEntry(
                helpers.generate_object_hash(),
                memoryview(tx_contents)
            )
            self.gateway_node.feed_manager.publish_to_feed(
                "newTxs", published_message
            )

            subscription_message = await ws.get_next_subscription_notification_by_id(
                subscription_id
            )
            self.assertEqual(
                subscription_id, subscription_message.subscription_id
            )
            self.assertEqual(
                published_message.tx_hash, subscription_message.notification["tx_hash"]
            )
            self.assertEqual(
                published_message.tx_contents, subscription_message.notification["tx_contents"]
            )

            task = asyncio.create_task(
                ws.get_next_subscription_notification_by_id(subscription_id)
            )
            await connection_handler.close()
            await asyncio.sleep(0.01)

            exception = task.exception()
            self.assertIsInstance(exception, WsException)

    @async_test
    async def test_multiple_rpc_calls_mixed_response(self):
        # unlikely to ever actually happen in practice, but should be handled
        async with WsProvider(self.ws_uri) as ws:
            self.assertEqual(1, len(self.server._connections))

            connection_handler = self.server._connections[0]
            connection_handler.rpc_handler.handle_request = AsyncMock()

            subscribe_task_1 = asyncio.create_task(
                ws.call(RpcRequestType.SUBSCRIBE, ["newTxs"], request_id="123")
            )
            await asyncio.sleep(0)
            subscribe_task_2 = asyncio.create_task(
                ws.call(RpcRequestType.SUBSCRIBE, ["newTxs"], request_id="124")
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
