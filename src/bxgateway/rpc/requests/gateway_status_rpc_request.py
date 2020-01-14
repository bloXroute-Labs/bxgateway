import asyncio
from typing import TYPE_CHECKING, Union, List, Dict, Any
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk

from bxutils.encoding.json_encoder import EnhancedJSONEncoder

from bxgateway.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxgateway.rpc.rpc_request_type import RpcRequestType
from bxgateway.utils.logging.status import status_log
from bxgateway.rpc import rpc_constants
from bxgateway.rpc.gateway_status_details_level import GatewayStatusDetailsLevel

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayStatusRpcRequest(AbstractRpcRequest):
    DETAILS_LEVEL = rpc_constants.DETAILS_LEVEL_PARAMS_KEY
    help = {
        "params": "Optional - {}: {}.".format(
            DETAILS_LEVEL, list(GatewayStatusDetailsLevel.__members__.keys())
        )
    }

    def __init__(
            self,
            method: RpcRequestType,
            node: "AbstractGatewayNode",
            request_id: str = "",
            params: Union[Dict[str, Any], List[Any], None] = None
    ):
        if params is None:
            params = {self.DETAILS_LEVEL: GatewayStatusDetailsLevel.SUMMARY.name}
        super().__init__(method, node, request_id, params)
        self._json_encoder = EnhancedJSONEncoder()
        self._details_level = GatewayStatusDetailsLevel.SUMMARY

    async def process_request(self) -> Response:
        try:
            self._details_level = GatewayStatusDetailsLevel[self.params[self.DETAILS_LEVEL].upper()]  # pyre-ignore
        except (KeyError, TypeError):
            pass
        loop = asyncio.get_event_loop()
        opts = self._node.opts
        diagnostics = await loop.run_in_executor(
            self._node.requester.thread_pool,
            status_log.get_diagnostics,
            opts.use_extensions,
            opts.source_version,
            opts.external_ip,
            opts.continent,
            opts.country
        )
        if self._details_level == GatewayStatusDetailsLevel.SUMMARY:
            data = diagnostics.summary
        else:
            data = diagnostics
        result = self._json_encoder.as_dict(data)
        return self._format_response(result, HTTPOk)