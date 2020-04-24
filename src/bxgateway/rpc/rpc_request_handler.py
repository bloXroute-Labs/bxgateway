from typing import Dict, TYPE_CHECKING, Type

from bxcommon.rpc.rpc_request_handler import RpcRequestHandler
from bxcommon.rpc.requests.blxr_transaction_rpc_request import BlxrTransactionRpcRequest
from bxgateway.rpc.requests.bdn_performance_rpc_request import BdnPerformanceRpcRequest
from bxgateway.rpc.requests.gateway_status_rpc_request import GatewayStatusRpcRequest
from bxgateway.rpc.requests.gateway_stop_rpc_request import GatewayStopRpcRequest
from bxgateway.rpc.requests.gateway_memory_rpc_request import GatewayMemoryRpcRequest
from bxgateway.rpc.requests.gateway_peers_rpc_request import GatewayPeersRpcRequest

from bxutils import logging

from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractGatewayRpcRequest
from bxcommon.rpc.rpc_request_type import RpcRequestType

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class RpcGatewayRequestHandler(RpcRequestHandler):

    _node: "AbstractGatewayNode"

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self._node = node
        # pyre-fixme[8]: Incompatible attribute type [8]: Attribute `_request_handlers` declared in class
        #  `RpcGatewayRequestHandler` has type `Dict[RpcRequestType, Type[AbstractGatewayRpcRequest]]` but is used
        #  as type `Dict[RpcRequestType, Type[typing.Union[BdnPerformanceRpcRequest, BlxrTransactionRpcRequest,
        #  GatewayMemoryRpcRequest, GatewayPeersRpcRequest, GatewayStatusRpcRequest, GatewayStopRpcRequest]]]
        self._request_handlers: Dict[RpcRequestType, Type[AbstractGatewayRpcRequest]] = {
            RpcRequestType.BLXR_TX: BlxrTransactionRpcRequest,
            RpcRequestType.GATEWAY_STATUS: GatewayStatusRpcRequest,
            RpcRequestType.STOP: GatewayStopRpcRequest,
            RpcRequestType.MEMORY: GatewayMemoryRpcRequest,
            RpcRequestType.PEERS: GatewayPeersRpcRequest,
            RpcRequestType.BDN_PERFORMANCE: BdnPerformanceRpcRequest
        }
        self.request_id = ""
