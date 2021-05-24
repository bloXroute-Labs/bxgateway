import asyncio
from dataclasses import dataclass
from typing import Any
from unittest import skip

import orjson
from mock import MagicMock, patch

from bxutils.encoding.json_encoder import Case

# TODO: remove try-catch when removing py3.7 support
try:
    from asyncio.exceptions import TimeoutError
except ImportError:
    from asyncio.futures import TimeoutError

import websockets

from bxgateway.gateway_opts import GatewayOpts
from bxgateway.testing import gateway_helpers
from bxcommon import constants
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.test_utils.helpers import async_test
from bxcommon.feed.feed import Feed, FeedKey
from bxcommon.feed.feed_manager import FeedManager
from bxgateway.rpc.ws.ws_server import WsServer
from bxgateway.testing.abstract_gateway_rpc_integration_test import \
    AbstractGatewayRpcIntegrationTest


class TestFeed(Feed[Any, Any]):
    def serialize(self, raw_message: int) -> int:
        return raw_message


class WsServerTest(AbstractGatewayRpcIntegrationTest):
    @async_test
    async def setUp(self) -> None:
        await super().setUp()
        self.feed_manager = FeedManager(self.gateway_node)
        self.server = WsServer(
            constants.LOCALHOST, 8005, self.feed_manager, self.gateway_node, Case.SNAKE
        )
        self.ws_uri = f"ws://{constants.LOCALHOST}:8005"
        await self.server.start()

    def get_gateway_opts(self) -> GatewayOpts:
        super().get_gateway_opts()
        return gateway_helpers.get_gateway_opts(
            8000,
            blockchain_protocol="Ethereum",
            account_model=self._account_model,
        )

    async def request(self, req: BxJsonRpcRequest) -> JsonRpcResponse:
        async with websockets.connect(self.ws_uri) as ws:
            await ws.send(req.to_jsons())
            response = await ws.recv()
            return JsonRpcResponse.from_jsons(response)

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
        self.gateway_node.account_model.get_feed_service_config_by_name = MagicMock(
            return_value=self.gateway_node.account_model.new_transaction_streaming
        )
        feed = TestFeed("foo")
        feed.feed_key = FeedKey("foo", self.gateway_node.network_num)

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
                BxJsonRpcRequest(
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
                BxJsonRpcRequest(
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
    async def test_camel_case(self):
        self.gateway_node.account_model.get_feed_service_config_by_name = MagicMock(
            return_value=self.gateway_node.account_model.new_transaction_streaming
        )
        await self.server.stop()

        self.server = WsServer(
            constants.LOCALHOST, 8005, self.feed_manager, self.gateway_node, Case.CAMEL
        )
        await self.server.start()

        feed = TestFeed("foo")
        feed.feed_key = FeedKey("foo", self.gateway_node.network_num)

        self.feed_manager.register_feed(feed)

        @dataclass
        class DataEntry:
            field_one: str

        def publish():
            feed.publish(DataEntry("123"))
            feed.publish(DataEntry("234"))

        async with websockets.connect(self.ws_uri) as ws:
            asyncio.get_event_loop().call_later(
                0.1, publish
            )
            await ws.send(
                BxJsonRpcRequest(
                    "2", RpcRequestType.SUBSCRIBE, ["foo", {}]
                ).to_jsons()
            )

            response = await ws.recv()
            parsed_response = JsonRpcResponse.from_jsons(response)

            self.assertEqual("2", parsed_response.id)
            self.assertIsNotNone("1", parsed_response.result)
            subscriber_id = parsed_response.result

            self._assert_notification({"fieldOne": "123"}, subscriber_id, await ws.recv())
            self._assert_notification({"fieldOne": "234"}, subscriber_id, await ws.recv())

    @skip("Reenable this when orjson library upgraded")
    @async_test
    async def test_serialization_caching(self):
        old_to_json_bytes_split = BxJsonRpcRequest.to_json_bytes_split_serialization
        old_to_json_bytes_cached = BxJsonRpcRequest.to_json_bytes_with_cached_result

        # cache hit, cache miss
        byte_serialize_count = [0, 0]

        def to_json_bytes_split(calling_self, case):
            byte_serialize_count[0] += 1
            return old_to_json_bytes_split(calling_self, case)

        def to_json_bytes_cached(calling_self, case, cached):
            byte_serialize_count[1] += 1
            return old_to_json_bytes_cached(calling_self, case, cached)

        BxJsonRpcRequest.to_json_bytes_split_serialization = to_json_bytes_split
        BxJsonRpcRequest.to_json_bytes_with_cached_result = to_json_bytes_cached

        self.gateway_node.account_model.get_feed_service_config_by_name = MagicMock(
            return_value=self.gateway_node.account_model.new_transaction_streaming
        )
        feed = TestFeed("foo")
        feed.FIELDS = ["foo", "bar"]
        feed.ALL_FIELDS = ["foo", "bar"]
        self.feed_manager.register_feed(feed)

        def publish():
            feed.publish({"foo": 1, "bar": 2})

        ws = await websockets.connect(self.ws_uri)
        ws_same = await websockets.connect(self.ws_uri)
        ws_same_2 = await websockets.connect(self.ws_uri)
        ws_diff = await websockets.connect(self.ws_uri)

        try:

            asyncio.get_event_loop().call_later(
                0.1, publish
            )

            # subscribe all clients
            await ws.send(
                BxJsonRpcRequest(
                    "2", RpcRequestType.SUBSCRIBE, ["foo", {"include": ["foo"]}]
                ).to_jsons()
            )
            await ws_same.send(
                BxJsonRpcRequest(
                    "2", RpcRequestType.SUBSCRIBE, ["foo", {"include": ["foo"]}]
                ).to_jsons()
            )
            await ws_same_2.send(
                BxJsonRpcRequest(
                    "2", RpcRequestType.SUBSCRIBE, ["foo", {"include": ["foo"]}]
                ).to_jsons()
            )
            await ws_diff.send(
                BxJsonRpcRequest(
                    "2", RpcRequestType.SUBSCRIBE, ["foo", {"include": ["bar"]}]
                ).to_jsons()
            )

            # receive all subscription responses
            ws_subscription_id = self._get_subscription_id(await ws.recv())
            ws_same_subscription_id = self._get_subscription_id(await ws_same.recv())
            ws_same_2_subscription_id = self._get_subscription_id(await ws_same_2.recv())
            ws_diff_subscription_id = self._get_subscription_id(await ws_diff.recv())

            byte_serialize_count[0] = 0

            self._assert_notification({"foo": 1}, ws_subscription_id, await ws.recv())
            self._assert_notification({"foo": 1}, ws_same_subscription_id, await ws_same.recv())
            self._assert_notification({"foo": 1}, ws_same_2_subscription_id, await ws_same_2.recv())
            self._assert_notification({"bar": 2}, ws_diff_subscription_id, await ws_diff.recv())

            self.assertEqual(2, byte_serialize_count[0])
            self.assertEqual(2, byte_serialize_count[1])

        finally:
            await ws.close()
            await ws_same.close()
            await ws_diff.close()

    @async_test
    async def tearDown(self) -> None:
        await self.server.stop()

    def _get_subscription_id(self, message: str) -> str:
        parsed_response = JsonRpcResponse.from_jsons(message)
        subscriber_id = parsed_response.result
        return subscriber_id

    def _assert_notification(
        self, expected_result: Any, subscriber_id: str, message: str
    ):
        parsed_notification = BxJsonRpcRequest.from_jsons(message)
        self.assertIsNone(parsed_notification.id)
        self.assertEqual(RpcRequestType.SUBSCRIBE, parsed_notification.method)
        self.assertEqual(subscriber_id, parsed_notification.params["subscription"])
        self.assertEqual(expected_result, parsed_notification.params["result"])
