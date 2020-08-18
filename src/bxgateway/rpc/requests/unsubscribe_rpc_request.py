from typing import TYPE_CHECKING, Callable, Optional

from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxgateway.feed.feed_manager import FeedManager

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class UnsubscribeRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {
        "params": "[subscription_id]: Subscription ID returned from subscribe call",
        "description": "Unsubscribe from provided subscription ID"
    }

    def __init__(
        self,
        request: BxJsonRpcRequest,
        node: "AbstractGatewayNode",
        feed_manager: FeedManager,
        unsubscribe_handler: Callable[[str], Optional[str]]
    ) -> None:
        self.feed_manager = feed_manager
        self.unsubscribe_handler = unsubscribe_handler
        self.subscriber_id = ""
        super().__init__(request, node)
        assert self.subscriber_id != ""

    def validate_params(self) -> None:
        params = self.params
        if (
            not isinstance(params, list)
            or len(params) != 1
            or not isinstance(params[0], str)
        ):
            raise RpcInvalidParams(
                self.request_id,
                "Unsubscribe RPC request params must be a list of length 1."
            )
        self.subscriber_id = params[0]

    async def process_request(self) -> JsonRpcResponse:
        feed_name = self.unsubscribe_handler(self.subscriber_id)
        if feed_name is None:
            raise RpcInvalidParams(
                self.request_id,
                f"Subscriber {self.subscriber_id} was not found."
            )
        self.feed_manager.unsubscribe_from_feed(feed_name, self.subscriber_id)
        return JsonRpcResponse(self.request_id, True)
