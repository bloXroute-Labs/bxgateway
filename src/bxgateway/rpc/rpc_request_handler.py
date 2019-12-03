from typing import Union, Any, Dict, List, TYPE_CHECKING, Type
from json.decoder import JSONDecodeError
from aiohttp.web import Request, Response
from aiohttp.web_exceptions import HTTPBadRequest
from bxgateway.rpc.blxr_transaction_rpc_request import BlxrTransactionRpcRequest
from bxgateway.rpc.gateway_status_rpc_request import GatewayStatusRpcRequest

from bxutils import logging

from bxgateway.rpc.abstract_rpc_request import AbstractRpcRequest
from bxgateway.rpc.rpc_request_type import RpcRequestType

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class RPCRequestHandler:

    CONTENT_TYPE: str = "Content-Type"
    PLAIN: str = "text/plain"
    REQUEST_ID: str = "id"
    REQUEST_METHOD: str = "method"
    REQUEST_PARAMS: str = "params"
    request_id: str
    _node: "AbstractGatewayNode"

    def __init__(self, node: "AbstractGatewayNode"):
        self._node = node
        self._request_handlers: Dict[RpcRequestType, Type[AbstractRpcRequest]] = {
            RpcRequestType.BLXR_TX: BlxrTransactionRpcRequest,
            RpcRequestType.GATEWAY_STATUS: GatewayStatusRpcRequest
        }
        self.request_id = ""

    async def handle_request(self, request: Request) -> Response:
        self.request_id = ""
        try:
            content_type = request.headers[self.CONTENT_TYPE]
        except KeyError:
            raise HTTPBadRequest(text=f"Request must have a {self.CONTENT_TYPE} header!")
        if content_type != self.PLAIN:
            raise HTTPBadRequest(text=f"{self.CONTENT_TYPE} must be {self.PLAIN}, not {content_type}!")
        try:
            payload = await request.json()
        except JSONDecodeError:
            body = await request.text()
            raise HTTPBadRequest(text=f"Request body: {body}, is not JSON serializable!")
        method = None
        try:
            method = payload[self.REQUEST_METHOD]
            method = RpcRequestType[method.upper()]  # pyre-ignore
        except KeyError:
            if method is None:
                raise HTTPBadRequest(text=f"RPC request does not contain a method!")
            else:
                possible_values = [rpc_type.lower() for rpc_type in RpcRequestType.__members__.keys()]
                raise HTTPBadRequest(
                    text=f"RPC method: {method} is not recognized (possible_values: {possible_values})."
                )
        self.request_id = payload.get(self.REQUEST_ID, "")
        request_params = payload.get(self.REQUEST_PARAMS, None)
        rpc_request = self._get_rpc_request(method, request_params)
        return await rpc_request.process_request()

    async def help(self) -> List[Any]:
        return [
            {
                "method": method.lower(),
                "id": "Optional - [unique request identifier string].",
                "params": self._request_handlers[RpcRequestType[method]].help["params"]
            }
            for method in RpcRequestType.__members__.keys()
        ]

    def _get_rpc_request(
            self,
            method: RpcRequestType,
            request_params: Union[Dict[str, Any], List[Any], None]
    ) -> AbstractRpcRequest:
        return self._request_handlers[method](method, self._node, self.request_id, request_params)  # pyre-ignore
