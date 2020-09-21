import asyncio
from asyncio import QueueEmpty

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_config_base import BdnServiceModelConfigBase
from bxcommon.models.bdn_service_model_base import BdnServiceModelBase
from bxcommon.models.bdn_service_type import BdnServiceType
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcError, RpcErrorCode
from bxcommon.rpc.json_rpc_response import JsonRpcResponse

from bxgateway.feed.eth.eth_on_block_feed import (
    EthOnBlockFeed,
    EventNotification,
    EventType,
)
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.testing.mocks.mock_eth_ws_proxy_publisher import MockEthWsProxyPublisher

from bxutils import constants as utils_constants


def _iter_queue(queue):
    while True:
        try:
            item = queue.get_nowait()
            yield item
        except QueueEmpty:
            break


class EthOnBlockFeedTest(AbstractTestCase):
    def setUp(self) -> None:
        self._account_model = BdnAccountModelBase(
            account_id="",
            logical_account_name="",
            certificate="",
            expire_date=utils_constants.DEFAULT_EXPIRATION_DATE.isoformat(),
            cloud_api=BdnServiceModelConfigBase(
                msg_quota=None,
                permit=BdnServiceModelBase(service_type=BdnServiceType.PERMIT),
                expire_date=utils_constants.DEFAULT_EXPIRATION_DATE.isoformat(),
            ),
            blockchain_protocol="Ethereum",
            blockchain_network="Mainnet",
        )
        self.rpc_port = helpers.get_free_port()
        opts = gateway_helpers.get_gateway_opts(
            8000,
            rpc_port=self.rpc_port,
            account_model=self._account_model,
            blockchain_protocol="Ethereum",
        )
        self.node = MockGatewayNode(opts)
        self.node.eth_ws_proxy_publisher = MockEthWsProxyPublisher(None, None, None, self.node)
        self.node.eth_ws_proxy_publisher.call_rpc = AsyncMock(
            return_value=JsonRpcResponse(request_id=1)
        )
        self.sut = EthOnBlockFeed(self.node)

    def test_subscribe(self):
        # test validations
        # multipile calls with the same name
        with self.assertRaises(RpcInvalidParams) as e:
            _ = self.sut.subscribe({"call_params": [{"name": "1"}, {"name": "1"}]})

        data = "0x6d4ce63c"
        subscriber = self.sut.subscribe({"call_params": [{"data": data, "name": "1"}]})
        self.assertEqual(subscriber.options["calls"]["1"].call_payload["data"], data)

    @async_test
    async def test_publish(self):
        data = "0x6d4ce63c"
        block_number = 999
        subscriber = self.sut.subscribe({"call_params": [{"data": data, "name": "1"}]})
        await self._publish_to_feed(block_number)
        self.node.eth_ws_proxy_publisher.call_rpc.mock.assert_called_with(
            "eth_call", [{"data": data}, hex(block_number)]
        )
        self.assertEqual(subscriber.messages.qsize(), 2)
        published_message = subscriber.messages.get_nowait()
        self.assertEqual(published_message["response"], {})
        self.assertEqual(published_message["block_height"], block_number)
        self.assertEqual(published_message["name"], "1")

    @async_test
    async def test_publish_multiple_subscribers(self):
        subscribers = []
        for i in range(10):
            data = hex(i)
            subscribers.append(
                self.sut.subscribe({"call_params": [{"data": data, "name": data}]})
            )
        await self._publish_to_feed()
        calls = self.node.eth_ws_proxy_publisher.call_rpc.mock.call_args_list
        self.assertEqual(len(calls), 10)
        for subscriber in subscribers:
            self.assertEqual(subscriber.messages.qsize(), 2)
            msg = subscriber.messages.get_nowait()
            self.assertIn(msg["name"], subscriber.options["calls"].keys())
            msg = subscriber.messages.get_nowait()
            self.assertEqual(msg["name"], str(EventType.TASK_COMPLETED_EVENT))

    @async_test
    async def test_publish_multipile_calls_for_subscriber(self):
        calls_number = 10
        subscriber = self.sut.subscribe(
            {
                "call_params": [
                    {"data": hex(i), "name": hex(i)} for i in range(calls_number)
                ]
            }
        )
        await self._publish_to_feed()
        calls = self.node.eth_ws_proxy_publisher.call_rpc.mock.call_args_list
        self.assertEqual(len(calls), calls_number)
        self.assertEqual(subscriber.messages.qsize(), calls_number + 1)

    @async_test
    async def test_subscribe_update(self):
        subscriber = self.sut.subscribe(
            {"call_params": [{"data": hex(1), "name": hex(1)}]}
        )
        subscription_id = subscriber.subscription_id
        # test adding calls to subscription
        self.sut.subscribe(
            {
                "subscription_id": subscription_id,
                "call_params": [{"data": hex(2), "name": hex(2)}],
            }
        )
        await self._publish_to_feed(1)
        self.assertEqual(subscriber.messages.qsize(), 3)

        for _ in _iter_queue(subscriber.messages):
            pass
        # test removing calls from a subscription
        self.sut.subscribe(
            {
                "subscription_id": subscription_id,
                "call_params": [{"data": hex(2), "name": hex(2), "active": False}],
            }
        )
        await self._publish_to_feed(2)
        self.assertEqual(subscriber.messages.qsize(), 2)

    @async_test
    async def test_publish_same_block(self):
        subscriber = self.sut.subscribe(
            {"call_params": [{"data": hex(1), "name": hex(1)}]}
        )
        await self._publish_to_feed(5)
        self.assertEqual(subscriber.messages.qsize(), 2)

        # clear subscriber queue
        for _ in _iter_queue(subscriber.messages):
            pass

        # same block ignore
        await self._publish_to_feed(5)
        self.assertEqual(subscriber.messages.qsize(), 0)

        # older block ignore
        await self._publish_to_feed(4)
        self.assertEqual(subscriber.messages.qsize(), 0)

    @async_test
    async def test_error_response(self):
        call_name = "test_eth_call_1"
        subscriber = self.sut.subscribe(
            {"call_params": [{"data": hex(1), "name": call_name}]}
        )
        self.node.eth_ws_proxy_publisher.call_rpc = AsyncMock(
            side_effect=RpcError(RpcErrorCode.SERVER_ERROR, "1", None, "out of gas")
        )

        # test ethNode Error response
        # the error will be sent to the subscriber
        # the specific call will be disabled and a notification will be sent to the subscriber
        await self._publish_to_feed(1)
        self.assertEqual(subscriber.messages.qsize(), 3)
        expected_response_names = {
            call_name,
            str(EventType.TASK_COMPLETED_EVENT),
            str(EventType.TASK_DISABLED_EVENT),
        }
        for msg in _iter_queue(subscriber.messages):
            self.assertIn(msg["name"], expected_response_names)
            expected_response_names.discard(msg["name"])
        self.assertFalse(expected_response_names)

        # expect only TaskCompleted response when the subscribed calls are disabled
        await self._publish_to_feed(2)
        self.assertEqual(subscriber.messages.qsize(), 1)

    @async_test
    async def test_publish_get_balance(self):
        block_number = 999
        await self._test_custom_call(
            "eth_getBalance",
            block_number=block_number,
            subscribe_payload={"address": "fake_address"},
            request_payload=["fake_address", hex(block_number)])

    @async_test
    async def test_publish_get_tx_count(self):
        block_number = 999
        await self._test_custom_call(
            "eth_getTransactionCount",
            block_number=block_number,
            subscribe_payload={"address": "fake_address"},
            request_payload=["fake_address", hex(block_number)])

    @async_test
    async def test_publish_get_code(self):
        block_number = 999
        await self._test_custom_call(
            "eth_getCode",
            block_number=block_number,
            subscribe_payload={"address": "fake_address"},
            request_payload=["fake_address", hex(block_number)])

    @async_test
    async def test_publish_get_storage_at(self):
        block_number = 999
        await self._test_custom_call(
            "eth_getStorageAt",
            block_number=block_number,
            subscribe_payload={"address": "fake_address", "pos": "0x"},
            request_payload=["fake_address", "0x", hex(block_number)])

    async def _test_custom_call(self, method, block_number, subscribe_payload, request_payload):
        subscriber = self.sut.subscribe(
            {"call_params": [{"name": "1", "method": method, **subscribe_payload}]})
        await self._publish_to_feed(block_number)
        self.node.eth_ws_proxy_publisher.call_rpc.mock.assert_called_with(
            method, request_payload
        )
        self.assertEqual(subscriber.messages.qsize(), 2)
        published_message = subscriber.messages.get_nowait()
        self.assertEqual(published_message["response"], {})
        self.assertEqual(published_message["block_height"], block_number)
        self.assertEqual(published_message["name"], "1")

    async def _publish_to_feed(self, block_height=1):
        self.sut.publish(EventNotification(block_height=block_height))
        await asyncio.sleep(0.01)
