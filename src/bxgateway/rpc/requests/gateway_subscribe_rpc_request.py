from typing import TYPE_CHECKING, Callable

from bxcommon.feed.feed import FeedKey
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.requests.subscribe_rpc_request import SubscribeRpcRequest
from bxcommon.feed.feed_manager import FeedManager
from bxcommon.feed.subscriber import Subscriber

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewaySubscribeRpcRequest(SubscribeRpcRequest):
    def __init__(
        self,
        request: BxJsonRpcRequest,
        node: "AbstractGatewayNode",
        feed_manager: FeedManager,
        subscribe_handler: Callable[[Subscriber, FeedKey], None],
        feed_network: int = 0
    ) -> None:
        super().__init__(request, node, feed_manager, subscribe_handler, feed_network, node.account_model)
