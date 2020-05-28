import asyncio
import json
from typing import Any
# TODO: remove try-catch when removing py3.7 support
try:
    from asyncio.exceptions import TimeoutError
except ImportError:
    from asyncio.futures import TimeoutError

import websockets

from bxgateway.gateway_opts import GatewayOpts
from bxgateway.testing import gateway_helpers
from bxcommon import constants
from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.test_utils.helpers import async_test
from bxgateway.feed.feed import Feed
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.testing.abstract_gateway_rpc_integration_test import \
    AbstractGatewayRpcIntegrationTest
from bxgateway.rpc.ws.ws_server import WsServer


class WsServerTest(AbstractGatewayRpcIntegrationTest):
    @async_test
    async def setUp(self) -> None:
        await super().setUp()
        self.feed_manager = FeedManager()
        self.server = WsServer(
            constants.LOCALHOST, 8005, self.feed_manager, self.gateway_node
        )
        self.ws_uri = f"ws://{constants.LOCALHOST}:8005"
        await self.server.start()

    def get_gateway_opts(self) -> GatewayOpts:
        return gateway_helpers.get_gateway_opts(8000, )

    async def request(self, req: JsonRpcRequest) -> JsonRpcResponse:
        async with websockets.connect(self.ws_uri) as ws:
            await ws.send(req.to_jsons())
            return JsonRpcResponse.from_jsons(await ws.recv())

    @async_test
    async def test_startup(self):
        async with websockets.connect(self.ws_uri) as _ws:
            await asyncio.sleep(0.01)

            self.assertEqual(1, len(self.server._connections))
            connection = self.server._connections[0]
            self.assertFalse(connection.request_handler.done())
            self.assertFalse(connection.publish_handler.done())

    @async_test
    async def test_subscribe_and_unsubscribe(self):
        feed: Feed[int] = Feed("foo")
        self.feed_manager.register_feed(feed)

        def publish():
            feed.publish(1)
            feed.publish(2)
            feed.publish(3)

        async with websockets.connect(self.ws_uri) as ws:
            asyncio.get_event_loop().call_later(
                0.1, publish
            )
            await ws.send(
                JsonRpcRequest(
                    "2", RpcRequestType.SUBSCRIBE, ["foo", {}]
                ).to_jsons()
            )
            response = await ws.recv()
            parsed_response = JsonRpcResponse.from_jsons(response)

            self.assertEqual("2", parsed_response.id)
            self.assertIsNotNone("1", parsed_response.result)
            subscriber_id = parsed_response.result

            self._assert_notification(1, subscriber_id, await ws.recv())
            self._assert_notification(2, subscriber_id, await ws.recv())
            self._assert_notification(3, subscriber_id, await ws.recv())

            await ws.send(
                JsonRpcRequest(
                    "3", RpcRequestType.UNSUBSCRIBE, [subscriber_id]
                ).to_jsons()
            )
            response = await ws.recv()
            parsed_response = JsonRpcResponse.from_jsons(response)
            self.assertEqual("3", parsed_response.id)
            self.assertTrue(parsed_response.result)

            publish()
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(ws.recv(), 0.1)

    @async_test
    async def tearDown(self) -> None:
        await self.server.stop()

    def _assert_notification(
        self, expected_result: Any, subscriber_id: str, message: str
    ):
        parsed_notification = JsonRpcRequest.from_jsons(message)
        self.assertIsNone(parsed_notification.id)
        self.assertEqual(RpcRequestType.SUBSCRIBE, parsed_notification.method)
        self.assertEqual(subscriber_id, parsed_notification.params["subscription"])
        self.assertEqual(expected_result, parsed_notification.params["result"])
