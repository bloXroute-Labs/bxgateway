from abc import abstractmethod
from typing import Union, Dict, Any, List, TYPE_CHECKING
from aiohttp.web import Response
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.rpc_request_type import RpcRequestType

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


# pyre-fixme[13]: Attribute `help` inherited from abstract class `AbstractRpcRequest` in class
#  `AbstractGatewayRpcRequest` to have type `Dict[str, typing.Any]` but is never initialized.
class AbstractGatewayRpcRequest(AbstractRpcRequest):
    _node: "AbstractGatewayNode"

    def __init__(
            self,
            method: RpcRequestType,
            node: "AbstractGatewayNode",
            request_id: str = "",
            params: Union[Dict[str, Any], List[Any], None] = None
    ):
        super().__init__(method, node, request_id, params)

    @abstractmethod
    async def process_request(self) -> Response:
        pass