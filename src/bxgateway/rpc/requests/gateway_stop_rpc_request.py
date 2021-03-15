from typing import TYPE_CHECKING

from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayStopRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {"params": "", "description": "shutdown request to the gateway"}

    def validate_params(self) -> None:
        pass

    async def process_request(self) -> JsonRpcResponse:
        self.node.should_force_exit = True
        return self.ok({})
