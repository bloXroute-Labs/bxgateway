import asyncio
import json
from asyncio import Future
from typing import TYPE_CHECKING, Dict, Any, NamedTuple, Optional, Union

from bxcommon.rpc.abstract_rpc_handler import AbstractRpcHandler
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.requests.transaction_status_rpc_request import TransactionStatusRpcRequest
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.models.blockchain_protocol import BlockchainProtocol

from bxgateway import gateway_constants, log_messages
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.feed.subscriber import Subscriber
from bxgateway.rpc.requests.bdn_performance_rpc_request import BdnPerformanceRpcRequest
from bxgateway.rpc.requests.gateway_blxr_transaction_rpc_request import \
    GatewayBlxrTransactionRpcRequest
from bxgateway.rpc.requests.gateway_memory_rpc_request import GatewayMemoryRpcRequest
from bxgateway.rpc.requests.gateway_memory_usage_report_rpc_request import GatewayMemoryUsageRpcRequest
from bxgateway.rpc.requests.gateway_peers_rpc_request import GatewayPeersRpcRequest
from bxgateway.rpc.requests.gateway_status_rpc_request import GatewayStatusRpcRequest
from bxgateway.rpc.requests.gateway_stop_rpc_request import GatewayStopRpcRequest
from bxgateway.rpc.requests.quota_usage_rpc_request import QuotaUsageRpcRequest
from bxgateway.rpc.requests.subscribe_rpc_request import SubscribeRpcRequest
from bxgateway.rpc.requests.unsubscribe_rpc_request import UnsubscribeRpcRequest
from bxgateway.rpc.requests.gateway_blxr_call_rpc_request import GatewayBlxrCallRpcRequest
from bxutils import logging
from bxutils.encoding.json_encoder import Case

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


logger = logging.get_logger(__name__)


class Subscription(NamedTuple):
    subscriber: Subscriber
    feed_name: str
    task: Future


class SubscriptionRpcHandler(AbstractRpcHandler["AbstractGatewayNode", Union[bytes, str], Union[bytes, str]]):
    feed_manager: FeedManager
    subscriptions: Dict[str, Subscription]
    subscribed_messages: 'asyncio.Queue[BxJsonRpcRequest]'

    def __init__(self, node: "AbstractGatewayNode", feed_manager: FeedManager, case: Case) -> None:
        super().__init__(node, case)
        self.request_handlers = {
            RpcRequestType.BLXR_TX: GatewayBlxrTransactionRpcRequest,
            RpcRequestType.BLXR_ETH_CALL: GatewayBlxrCallRpcRequest,
            RpcRequestType.GATEWAY_STATUS: GatewayStatusRpcRequest,
            RpcRequestType.STOP: GatewayStopRpcRequest,
            RpcRequestType.MEMORY: GatewayMemoryRpcRequest,
            RpcRequestType.PEERS: GatewayPeersRpcRequest,
            RpcRequestType.BDN_PERFORMANCE: BdnPerformanceRpcRequest,
            RpcRequestType.SUBSCRIBE: SubscribeRpcRequest,
            RpcRequestType.UNSUBSCRIBE: UnsubscribeRpcRequest,
            RpcRequestType.QUOTA_USAGE: QuotaUsageRpcRequest,
            RpcRequestType.MEMORY_USAGE: GatewayMemoryUsageRpcRequest,
            RpcRequestType.TX_STATUS: TransactionStatusRpcRequest,
        }

        self.feed_manager = feed_manager
        self.subscriptions = {}
        self.subscribed_messages = asyncio.Queue(
            gateway_constants.RPC_SUBSCRIBER_MAX_QUEUE_SIZE
        )
        self.disconnect_event = asyncio.Event()

    async def parse_request(self, request: Union[bytes, str]) -> Dict[str, Any]:
        return json.loads(request)

    def get_request_handler(self, request: BxJsonRpcRequest) -> AbstractRpcRequest:
        if request.method == RpcRequestType.SUBSCRIBE:
            return self._subscribe_request_factory(request, self.node)
        elif request.method == RpcRequestType.UNSUBSCRIBE:
            return self._unsubscribe_request_factory(request, self.node)
        else:
            request_handler_type = self.request_handlers[request.method]
            return request_handler_type(request, self.node)

    def serialize_response(self, response: JsonRpcResponse) -> str:
        return response.to_jsons(self.case)

    async def get_next_subscribed_message(self) -> BxJsonRpcRequest:
        return await self.subscribed_messages.get()

    async def handle_subscription(self, subscriber: Subscriber) -> None:
        while True:
            notification = await subscriber.receive()
            # subscription notifications are sent as JSONRPC requests
            next_message = BxJsonRpcRequest(
                None,
                RpcRequestType.SUBSCRIBE,
                {
                    "subscription": subscriber.subscription_id,
                    "result": notification
                }
            )
            if self.subscribed_messages.full():
                logger.error(
                    log_messages.BAD_RPC_SUBSCRIBER,
                    self.subscribed_messages.qsize(),
                    list(self.subscriptions.keys())
                )
                asyncio.create_task(self.async_close())
                return
            else:
                await self.subscribed_messages.put(next_message)

    async def wait_for_close(self) -> None:
        await self.disconnect_event.wait()

    async def async_close(self) -> None:
        self.close()

    def close(self) -> None:
        subscription_ids = list(self.subscriptions.keys())
        for subscription_id in subscription_ids:
            feed_name = self._on_unsubscribe(subscription_id)
            assert feed_name is not None
            self.feed_manager.unsubscribe_from_feed(feed_name, subscription_id)
        self.subscriptions = {}

        self.disconnect_event.set()

    def _on_new_subscriber(self, subscriber: Subscriber, feed_name: str) -> None:
        task = asyncio.ensure_future(self.handle_subscription(subscriber))
        self.subscriptions[subscriber.subscription_id] = Subscription(
            subscriber, feed_name, task
        )
        self.node.on_new_subscriber_request()

    def _on_unsubscribe(self, subscriber_id: str) -> Optional[str]:
        if subscriber_id in self.subscriptions:
            (subscriber, feed_name, task) = self.subscriptions.pop(subscriber_id)
            task.cancel()
            return feed_name
        return None

    def _subscribe_request_factory(
        self, request: BxJsonRpcRequest, node: "AbstractGatewayNode"
    ) -> AbstractRpcRequest:
        return SubscribeRpcRequest(
            request, node, self.feed_manager, self._on_new_subscriber
        )

    def _unsubscribe_request_factory(
        self, request: BxJsonRpcRequest, node: "AbstractGatewayNode"
    ) -> AbstractRpcRequest:
        return UnsubscribeRpcRequest(
            request, node, self.feed_manager, self._on_unsubscribe
        )

