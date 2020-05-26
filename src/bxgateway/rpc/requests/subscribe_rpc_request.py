from typing import TYPE_CHECKING, Callable

from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxgateway.feed.feed_manager import FeedManager
from bxgateway.feed.subscriber import Subscriber

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class SubscribeRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {
        "params": "[feed_name, {\"include\": [field_1, field_2]}.\n"
                  "Available feeds: unconfirmed_txs, pending_txs\n"
                  "Available fields: tx_hash, tx_contents (default: all)",
        "description": "Subscribe to a named feed for notifications"
    }

    def __init__(
        self,
        request: JsonRpcRequest,
        node: "AbstractGatewayNode",
        feed_manager: FeedManager,
        subscribe_handler: Callable[[Subscriber, str], None],
    ) -> None:
        self.feed_name = ""
        self.include = []
        self.feed_manager = feed_manager
        self.subscribe_handler = subscribe_handler

        super().__init__(request, node)

        assert self.feed_name != ""

    def validate_params(self) -> None:
        params = self.params
        if not isinstance(params, list) or len(params) != 2:
            raise RpcInvalidParams(
                self.request_id,
                "Subscribe RPC request params must be a list of length 2."
            )
        feed_name, options = params
        if feed_name not in self.feed_manager:
            raise RpcInvalidParams(
                self.request_id,
                f"{feed_name} is an invalid feed. "
                f"Available feeds: {self.feed_manager.feeds.keys()}"
            )
        self.feed_name = feed_name

        available_fields = self.feed_manager.get_feed_fields(feed_name)
        invalid_options = RpcInvalidParams(
            self.request_id,
            f"{options} is not a valid set of options. "
            "Valid format/fields: {\"include\": "f"{available_fields}""}."
        )
        if not isinstance(options, dict):
            raise invalid_options

        include = options.get("include", None)
        if include is not None:
            if not isinstance(include, list):
                raise invalid_options

            if any(
                included_field not in available_fields
                for included_field in include
            ):
                raise invalid_options

            self.include = include

    async def process_request(self) -> JsonRpcResponse:
        params = self.params
        assert isinstance(params, list)

        options = params[1]
        assert isinstance(options, dict)

        subscriber = self.feed_manager.subscribe_to_feed(
            self.feed_name, options.get("include", None)
        )
        assert subscriber is not None  # already validated
        self.subscribe_handler(subscriber, self.feed_name)

        return JsonRpcResponse(
            self.request_id,
            subscriber.subscription_id
        )


