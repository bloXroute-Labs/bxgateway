from typing import TYPE_CHECKING, Union, List, Dict, Any
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk


from bxutils.encoding.json_encoder import EnhancedJSONEncoder

from bxgateway.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxgateway.rpc.rpc_request_type import RpcRequestType


if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayPeersRpcRequest(AbstractRpcRequest):

    help = {
        "params": "",
        "description": "return gateway connected peers"
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
        data = []
        for conn in self._node.connection_pool:
            data.append(
                {
                    "id": conn.peer_id,
                    "type": str(conn.CONNECTION_TYPE),
                    "addr": conn.peer_desc,
                    "direction": str(conn.direction),
                    "state": str(conn.state),
                 })

        result = self._json_encoder.as_dict(data)
        return self._format_response(result, HTTPOk)
