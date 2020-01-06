from typing import TYPE_CHECKING, Union, List, Dict, Any
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk


from bxutils.encoding.json_encoder import EnhancedJSONEncoder

from bxcommon.utils.stats.memory_statistics_service import memory_statistics
from bxgateway.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxgateway.rpc.rpc_request_type import RpcRequestType


if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayMemoryRpcRequest(AbstractRpcRequest):

    help = {
        "params": "",
        "description": "return gateway node memory information"
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

    async def process_request(self) -> Response:
        self._node.connection_pool.log_connection_pool_mem_stats()
        self._node._tx_service.log_tx_service_mem_stats()

        data = memory_statistics.get_info()

        result = self._json_encoder.as_dict(data)
        return self._format_response(result, HTTPOk)
