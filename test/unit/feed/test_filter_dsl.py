from bxgateway.testing import gateway_helpers
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.feed.feed import Feed
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxutils.encoding.json_encoder import Case
from bxgateway.feed import filter_dsl
from bxutils import logging

logger = logging.get_logger()


def val_filter1(contents):
    assert contents == ["bar"]
    return contents


def val_filter2(contents):
    assert contents == ["hello"]
    return contents


REFORMAT_FILTER_MAP = {"field1": val_filter1, "field2": val_filter2}

FILTER_HANDLER_MAP = {}


def reformat_filter(filter_name: str, filter_contents):
    reformatter = REFORMAT_FILTER_MAP.get(filter_name, None)
    assert reformatter
    logger.error(
        f"validating with validator {reformatter.__name__} filters: {filter_contents}"
    )
    return reformatter(filter_contents)


def handle_filter(filter_name: str, filter_contents, tx) -> bool:
    handler = FILTER_HANDLER_MAP.get(filter_name, None)
    assert handler
    logger.debug(f"handling with handler {handler.__name__} filters: {filter_contents}")
    return handler(filter_contents, tx)


class TestFeed(Feed[str, str]):
    def serialize(self, raw_message: str) -> str:
        return raw_message

    def reformat_filters(self, filters):
        return filter_dsl.reformat(filters, reformat_filter)


class FilterDslTest(AbstractTestCase):
    def setUp(self) -> None:
        self.gateway = MockGatewayNode(gateway_helpers.get_gateway_opts(8000))
        self.feed_manager = FeedManager(self.gateway)
        self.rpc = SubscriptionRpcHandler(self.gateway, self.feed_manager, Case.SNAKE)

    @async_test
    async def test_subscribe_to_feed_with_filters(self):
        feed = TestFeed("bar")
        feed.FILTERS = ["field1", "field2"]

        self.feed_manager.register_feed(feed)
        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            ["bar", {"filters": {"AND": [{"field1": ["bar"]}]}}],
        )

        rpc_handler = self.rpc.get_request_handler(subscribe_request1)
        result = await rpc_handler.process_request()
        self.assertTrue(result)

    @async_test
    async def test_subscribe_to_feed_with_invalid_filters(self):
        feed = TestFeed("foo")
        feed.FILTERS = ["filter1", "OR", "filter2"]
        self.feed_manager.register_feed(feed)
        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            ["foo", {"filters": [{"field1": []}, "AND", {"field2": []}]}],
        )

        with self.assertRaises(RpcInvalidParams):
            rpc_handler = self.rpc.get_request_handler(subscribe_request1)
            await rpc_handler.process_request()

        subscribe_request2 = BxJsonRpcRequest(
            "1", RpcRequestType.SUBSCRIBE, ["foo", {"filters": [True, False, 1, 2]}]
        )
        with self.assertRaises(RpcInvalidParams):
            rpc_handler = self.rpc.get_request_handler(subscribe_request2)
            await rpc_handler.process_request()

        subscribe_request3 = BxJsonRpcRequest(
            "1", RpcRequestType.SUBSCRIBE, ["foo", {"filters": "OR"}]
        )
        with self.assertRaises(RpcInvalidParams):
            rpc_handler = self.rpc.get_request_handler(subscribe_request3)
            await rpc_handler.process_request()
        subscribe_request4 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            ["foo", {"filters": {"AND": [{"field1": ["bar"]}, {"OR": {"field2": ["hi"]}}]}}],
        )
        with self.assertRaises(RpcInvalidParams):
            rpc_handler = self.rpc.get_request_handler(subscribe_request4)
            await rpc_handler.process_request()
