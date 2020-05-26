import asyncio
from typing import TYPE_CHECKING, Union, List, Dict, Any

from bxcommon.rpc import rpc_constants
from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxgateway.rpc.gateway_status_details_level import GatewayStatusDetailsLevel
from bxgateway.utils.logging.status import status_log
from bxutils.encoding.json_encoder import EnhancedJSONEncoder

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayStatusRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {
        "params": f"Optional - {rpc_constants.DETAILS_LEVEL_PARAMS_KEY}: "
                  f"{list(GatewayStatusDetailsLevel.__members__.keys())}.",
        "description": "return status of the bloXroute Gateway"
    }

    def __init__(
        self,
        request: JsonRpcRequest,
        node: "AbstractGatewayNode",
    ):
        if request.params is None:
            request.params = {
                rpc_constants.DETAILS_LEVEL_PARAMS_KEY: GatewayStatusDetailsLevel.SUMMARY.name
            }
        super().__init__(request, node)
        self._json_encoder = EnhancedJSONEncoder()
        params = request.params
        assert isinstance(params, dict)
        self._details_level = params[rpc_constants.DETAILS_LEVEL_PARAMS_KEY]

    def validate_params(self) -> None:
        pass

    async def process_request(self) -> JsonRpcResponse:
        loop = asyncio.get_event_loop()
        opts = self.node.opts
        diagnostics = await loop.run_in_executor(
            self.node.requester.thread_pool,
            status_log.get_diagnostics,
            opts.use_extensions,
            opts.source_version,
            opts.external_ip,
            opts.continent,
            opts.country,
            opts.should_update_source_version,
            self.node.account_id,
            self.node.quota_level
        )
        if self._details_level == GatewayStatusDetailsLevel.SUMMARY:
            data = diagnostics.summary
        else:
            data = diagnostics
        result = self._json_encoder.as_dict(data)
        return self.ok(result)
