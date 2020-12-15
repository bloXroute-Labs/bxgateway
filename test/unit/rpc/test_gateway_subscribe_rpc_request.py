from unittest.mock import MagicMock

from bxcommon.feed.feed_manager import FeedManager
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_base import FeedServiceModelBase
from bxcommon.models.bdn_service_model_config_base import BdnFeedServiceModelConfigBase
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcAccountIdError
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxcommon.feed.new_transaction_feed import NewTransactionFeed
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxgateway.testing import gateway_helpers
from bxutils.encoding.json_encoder import Case


class GatewaySubscribeRpcRequestTest(AbstractTestCase):

    @async_test
    async def setUp(self) -> None:
        pub_key = "a04f30a45aae413d0ca0f219b4dcb7049857bc3f91a6351288cce603a2c9646294a02b987bf6586b370b2c22d74662355677007a14238bb037aedf41c2d08866"
        opts = gateway_helpers.get_gateway_opts(
            8000,
            include_default_eth_args=True,
            pub_key=pub_key,
            track_detailed_sent_messages=True
        )
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        node_ssl_service = MockNodeSSLService(EthGatewayNode.NODE_TYPE, MagicMock())
        self.node = EthGatewayNode(opts, node_ssl_service)
        self.feed_manager = FeedManager(self.node)
        self.node.feed_manager = self.feed_manager
        self.rpc = SubscriptionRpcHandler(self.node, self.feed_manager, Case.SNAKE)
        self.feed_name = NewTransactionFeed.NAME
        self.node.init_live_feeds()

        self.feed_service_model = FeedServiceModelBase(
            allow_filtering=True,
            available_fields=["all"]
        )
        self.base_feed_service_model = BdnFeedServiceModelConfigBase(
            expire_date="2999-01-01",
            feed=self.feed_service_model
        )
        self.node.account_model = BdnAccountModelBase(
            account_id="account_id",
            certificate="",
            logical_account_name="test",
            new_transaction_streaming=self.base_feed_service_model,
        )

    @async_test
    async def tearDown(self) -> None:
        pass

    @async_test
    async def test_subscribe_to_feed_with_no_includes_specified(self):
        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            [self.feed_name, {}],
        )

        rpc_handler = self.rpc.get_request_handler(subscribe_request1)
        result = await rpc_handler.process_request()
        self.assertTrue(result)

    @async_test
    async def test_subscribe_to_feed_with_all_available_fields(self):
        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            [self.feed_name, {"include": ["tx_hash", "tx_contents.gas_price"]}],
        )

        rpc_handler = self.rpc.get_request_handler(subscribe_request1)
        result = await rpc_handler.process_request()
        self.assertTrue(result)

    @async_test
    async def test_subscribe_to_feed_with_specific_available_field(self):
        feed_service_model = FeedServiceModelBase(
            allow_filtering=True,
            available_fields=["tx_contents.nonce"]
        )
        base_feed_service_model = BdnFeedServiceModelConfigBase(
            expire_date="2999-01-01",
            feed=feed_service_model
        )
        self.node.account_model.new_transaction_streaming = base_feed_service_model

        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            [self.feed_name, {"include": ["tx_contents.nonce"]}],
        )

        rpc_handler = self.rpc.get_request_handler(subscribe_request1)
        result = await rpc_handler.process_request()
        self.assertTrue(result)

    @async_test
    async def test_subscribe_to_feed_with_unavailable_field(self):
        feed_service_model = FeedServiceModelBase(
            allow_filtering=True,
            available_fields=["tx_hash", "tx_contents.nonce"]
        )
        base_feed_service_model = BdnFeedServiceModelConfigBase(
            expire_date="2999-01-01",
            feed=feed_service_model
        )
        self.node.account_model.new_transaction_streaming = base_feed_service_model

        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            [self.feed_name, {"include": ["tx_hash", "tx_contents.gas_price"]}],
        )

        with self.assertRaises(RpcInvalidParams):
            rpc_handler = self.rpc.get_request_handler(subscribe_request1)
            await rpc_handler.process_request()

    @async_test
    async def test_subscribe_to_feed_with_feed_not_set_in_account(self):
        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            ["pendingTxs", {}],
        )

        with self.assertRaises(RpcAccountIdError):
            rpc_handler = self.rpc.get_request_handler(subscribe_request1)
            await rpc_handler.process_request()

    @async_test
    async def test_subscribe_to_feed_no_feed_service(self):
        # allow feed if service is not defined (old account)
        self.node.account_model.new_transaction_streaming = BdnFeedServiceModelConfigBase(
                expire_date="2999-01-01"
            )

        subscribe_request1 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            [self.feed_name, {}],
        )
        subscribe_request2 = BxJsonRpcRequest(
            "1",
            RpcRequestType.SUBSCRIBE,
            [self.feed_name, {"include": ["tx_hash", "tx_contents.gas_price"]}],
        )

        rpc_handler = self.rpc.get_request_handler(subscribe_request1)
        result = await rpc_handler.process_request()
        self.assertTrue(result)
        rpc_handler = self.rpc.get_request_handler(subscribe_request2)
        result = await rpc_handler.process_request()
        self.assertTrue(result)
