from typing import TYPE_CHECKING, Union, List, Dict, Any
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk

from bxgateway.rpc.gateway_status_details_level import GatewayStatusDetailsLevel
from bxutils.encoding.json_encoder import EnhancedJSONEncoder

from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractGatewayRpcRequest
from bxcommon.rpc.rpc_request_type import RpcRequestType

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayStopRpcRequest(AbstractGatewayRpcRequest):
    help = {
        "params": "",
        "description": "shutdown request to the gateway"
    }

    def __init__(
            self,
            method: RpcRequestType,
            node: "AbstractGatewayNode",
            request_id: str = "",
            params: Union[Dict[str, Any], List[Any], None] = None
    ):
        super().__init__(method, node, request_id, params)
        self._json_encoder = EnhancedJSONEncoder()
        self._details_level = GatewayStatusDetailsLevel.SUMMARY

    async def process_request(self) -> Response:
        self._node.should_force_exit = True

        result = self._json_encoder.as_dict({})
        return self._format_response(result, HTTPOk)
