import json
from abc import ABCMeta, abstractmethod
from typing import Union, Dict, List, Any, TYPE_CHECKING, Type
from aiohttp.web_exceptions import HTTPSuccessful, HTTPOk
from aiohttp.web import Response

from bxgateway.rpc.rpc_request_type import RpcRequestType

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class AbstractRpcRequest(metaclass=ABCMeta):
    method: RpcRequestType
    request_id: str
    params: Union[Dict[str, Any], List[Any], None]
    help: Dict[str, Any]
    _node: "AbstractGatewayNode"

    def __init__(
            self,
            method: RpcRequestType,
            node: "AbstractGatewayNode",
            request_id: str = "",
            params: Union[Dict[str, Any], List[Any], None] = None
    ):
        self.method = method
        self.request_id = request_id
        if params is not None and isinstance(params, dict):
            params = {k.lower(): v for k, v in params.items()}
        self.params = params
        self._node = node

    @abstractmethod
    async def process_request(self) -> Response:
        pass

    def _format_response(
            self, result: Union[str, Dict[str, Any], List[Any]], response_type: Type[HTTPSuccessful] = HTTPOk
    ) -> Response:
        request_id = self.request_id
        response_json = {
            "result": result,
            "error": None,
            "code": response_type.status_code,
            "message": "",
            "id": request_id
        }
        return response_type(text=json.dumps(response_json))
