import asyncio
from asyncio import Future

from bxgateway.testing import gateway_helpers
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxcommon.feed.feed import Feed, FeedKey
from bxcommon.feed.feed_manager import FeedManager
from bxcommon.rpc.requests.subscribe_rpc_request import SubscribeRpcRequest
from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxutils.encoding.json_encoder import Case
from bxutils import logging

logger = logging.get_logger()


class TestFeed(Feed[str, str]):
    def serialize(self, raw_message: str) -> str:
        return raw_message


class SubscriptionRpcHandlerTest(AbstractTestCase):
    def setUp(self) -> None:
        self.gateway = MockGatewayNode(gateway_helpers.get_gateway_opts(8000))
        self.feed_manager = FeedManager(self.gateway)
        self.rpc = SubscriptionRpcHandler(self.gateway, self.feed_manager, Case.SNAKE)

    @async_test
    async def test_help(self):
        print(await self.rpc.help())

    @async_test
    async def test_subscribe_to_feed(self):
        feed = TestFeed("foo")
        feed.network_num = self.gateway.network_num
        feed.feed_key = FeedKey("foo", self.gateway.network_num)
        self.feed_manager.register_feed(feed)
        subscribe_request = BxJsonRpcRequest("1", RpcRequestType.SUBSCRIBE, ["foo", {}])

        rpc_handler = self.rpc.get_request_handler(subscribe_request)
        self.assertIsInstance(rpc_handler, SubscribeRpcRequest)

        result = await rpc_handler.process_request()
        self.assertIsNotNone(result.result)
        self.assertIsNone(result.error)
        subscriber_id = result.result

        self.assertEqual(1, self.feed_manager.get_feed(FeedKey("foo", self.gateway.network_num)).subscriber_count())
        self.assertEqual(1, len(self.rpc.subscriptions))
        self.assertIn(subscriber_id, self.rpc.subscriptions)

        next_message_task: Future[BxJsonRpcRequest] = asyncio.ensure_future(
            self.rpc.get_next_subscribed_message()
        )
        await asyncio.sleep(0)
        self.assertFalse(next_message_task.done())

        feed.publish("foobar")
        await asyncio.sleep(0)  # publish to subscriber
        await asyncio.sleep(0)  # subscriber publishes to queue

        self.assertTrue(next_message_task.done())
        next_message = next_message_task.result()

        self.assertEqual("2.0", next_message.json_rpc_version)
        self.assertEqual(RpcRequestType.SUBSCRIBE, next_message.method)
        self.assertEqual(subscriber_id, next_message.params["subscription"])
        self.assertEqual("foobar", next_message.params["result"])

    @async_test
    async def test_subscribe_to_feed_validation(self):
        feed = TestFeed("foo")
        feed.FIELDS = ["field1", "field2", "field3"]
        self.feed_manager.register_feed(feed)
        subscribe_request = BxJsonRpcRequest(
            "1", RpcRequestType.SUBSCRIBE, ["foo", {"include": ["field1", "field4"]}]
        )

        with self.assertRaises(RpcInvalidParams):
            rpc_handler = self.rpc.get_request_handler(subscribe_request)
            await rpc_handler.process_request()

    @async_test
    async def test_subscribe_to_feed_with_fields(self):
        feed = TestFeed("foo")
        feed.FIELDS = ["field1", "field2", "field3"]
        feed.feed_key = FeedKey("foo", self.gateway.network_num)
        self.feed_manager.register_feed(feed)
        subscribe_request = BxJsonRpcRequest(
            "1", RpcRequestType.SUBSCRIBE, ["foo", {"include": ["field1", "field2"]}]
        )

        rpc_handler = self.rpc.get_request_handler(subscribe_request)
        result = await rpc_handler.process_request()
        subscriber_id = result.result

        next_message_task: Future[BxJsonRpcRequest] = asyncio.ensure_future(
            self.rpc.get_next_subscribed_message()
        )

        feed.publish({"field1": "foo", "field2": "bar", "field3": "baz"})
        await asyncio.sleep(0)  # publish to subscriber
        await asyncio.sleep(0)  # subscriber publishes to queue

        self.assertTrue(next_message_task.done())
        next_message = next_message_task.result()

        self.assertEqual("2.0", next_message.json_rpc_version)
        self.assertEqual(RpcRequestType.SUBSCRIBE, next_message.method)
        self.assertEqual(subscriber_id, next_message.params["subscription"])

        expected_result = {
            "field1": "foo",
            "field2": "bar",
        }
        self.assertEqual(expected_result, next_message.params["result"])

    @async_test
    async def test_subscribe_to_multiple_feeds(self):
        feed1 = TestFeed("foo1")
        feed1.feed_key = FeedKey("foo1", self.gateway.network_num)
        feed2 = TestFeed("foo2")
        feed2.feed_key = FeedKey("foo2", self.gateway.network_num)
        feed3 = TestFeed("foo3")
        feed3.feed_key = FeedKey("foo3", self.gateway.network_num)
        for feed in [feed1, feed2, feed3]:
            self.feed_manager.register_feed(feed)

        subscribe_requests = [
            BxJsonRpcRequest("1", RpcRequestType.SUBSCRIBE, ["foo1", {}]),
            BxJsonRpcRequest("1", RpcRequestType.SUBSCRIBE, ["foo2", {}]),
            BxJsonRpcRequest("1", RpcRequestType.SUBSCRIBE, ["foo3", {}]),
        ]
        subscriber_ids = []
        for subscribe_request in subscribe_requests:
            rpc_handler = self.rpc.get_request_handler(subscribe_request)
            result = await rpc_handler.process_request()
            subscriber_ids.append(result.result)

        next_message_task: Future[BxJsonRpcRequest] = asyncio.ensure_future(
            self.rpc.get_next_subscribed_message()
        )
        await asyncio.sleep(0)
        self.assertFalse(next_message_task.done())

        feed2.publish("message 1")
        await asyncio.sleep(0)
        feed1.publish("message 2")
        await asyncio.sleep(0)
        feed2.publish("message 3")
        await asyncio.sleep(0)
        feed3.publish("message 4")
        await asyncio.sleep(0)

        self.assertTrue(next_message_task.done())

        message_1 = next_message_task.result()
        self.assertEqual(subscriber_ids[1], message_1.params["subscription"])
        self.assertEqual("message 1", message_1.params["result"])

        message_2 = await self.rpc.get_next_subscribed_message()
        self.assertEqual(subscriber_ids[0], message_2.params["subscription"])
        self.assertEqual("message 2", message_2.params["result"])

        message_3 = await self.rpc.get_next_subscribed_message()
        self.assertEqual(subscriber_ids[1], message_3.params["subscription"])
        self.assertEqual("message 3", message_3.params["result"])

        message_4 = await self.rpc.get_next_subscribed_message()
        self.assertEqual(subscriber_ids[2], message_4.params["subscription"])
        self.assertEqual("message 4", message_4.params["result"])

    @async_test
    async def test_unsubscribe(self):
        feed = TestFeed("foo")
        feed.network_num = self.gateway.network_num
        feed.feed_key = FeedKey("foo", self.gateway.network_num)
        self.feed_manager.register_feed(feed)

        subscribe_request = BxJsonRpcRequest("1", RpcRequestType.SUBSCRIBE, ["foo", {}])
        rpc_handler = self.rpc.get_request_handler(subscribe_request)
        result = await rpc_handler.process_request()
        subscriber_id = result.result

        # message is received
        next_message_task: Future[BxJsonRpcRequest] = asyncio.ensure_future(
            self.rpc.get_next_subscribed_message()
        )
        await asyncio.sleep(0)
        self.assertFalse(next_message_task.done())
        feed.publish("foobar")
        await asyncio.sleep(0)  # publish to subscriber
        await asyncio.sleep(0)  # subscriber publishes to queue
        self.assertTrue(next_message_task.done())

        unsubscribe_request = BxJsonRpcRequest(
            "1", RpcRequestType.UNSUBSCRIBE, [subscriber_id]
        )
        rpc_handler = self.rpc.get_request_handler(unsubscribe_request)
        result = await rpc_handler.process_request()
        self.assertTrue(result.result)

        next_message_task: Future[BxJsonRpcRequest] = asyncio.ensure_future(
            self.rpc.get_next_subscribed_message()
        )
        feed.publish("foobar_not_received")
        feed.publish("foobar_not_received_2")
        await asyncio.sleep(0)  # publish to subscriber
        await asyncio.sleep(0)  # subscriber publishes to queue
        self.assertFalse(next_message_task.done())  # no message

        self.assertEqual(0, len(self.rpc.subscriptions))
        self.assertEqual(0, self.feed_manager.get_feed(FeedKey("foo", self.gateway.network_num)).subscriber_count())

    @async_test
    async def test_close_bad_subscribers(self):
        self.rpc.subscribed_messages = asyncio.Queue(5)

        close_listener = asyncio.create_task(self.rpc.wait_for_close())

        feed1 = TestFeed("foo1")
        feed1.network_num = self.gateway.network_num
        feed1.feed_key = FeedKey("foo1", self.gateway.network_num)
        self.feed_manager.register_feed(feed1)

        rpc_request = BxJsonRpcRequest("1", RpcRequestType.SUBSCRIBE, ["foo1", {}])
        await self.rpc.get_request_handler(rpc_request).process_request()

        for i in range(10):
            feed1.publish(i)

        await asyncio.sleep(0.1)

        self.assertTrue(self.rpc.disconnect_event.is_set())
        self.assertTrue(close_listener.done())

    @async_test
    async def tearDown(self) -> None:
        self.rpc.close()
