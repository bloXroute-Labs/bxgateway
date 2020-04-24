from typing import Dict, TYPE_CHECKING, Type

from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.rpc_request_handler import RpcRequestHandler
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.rpc.requests.blxr_transaction_rpc_request import BlxrTransactionRpcRequest
from bxgateway.rpc.requests.bdn_performance_rpc_request import BdnPerformanceRpcRequest
from bxgateway.rpc.requests.gateway_status_rpc_request import GatewayStatusRpcRequest
from bxgateway.rpc.requests.gateway_stop_rpc_request import GatewayStopRpcRequest
from bxgateway.rpc.requests.gateway_memory_rpc_request import GatewayMemoryRpcRequest
from bxgateway.rpc.requests.gateway_peers_rpc_request import GatewayPeersRpcRequest

from bxutils import logging


if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class RpcGatewayRequestHandler(RpcRequestHandler):

    _node: "AbstractGatewayNode"

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self._node = node
        self._request_handlers: Dict[RpcRequestType, Type[AbstractRpcRequest]] = {
            RpcRequestType.BLXR_TX: BlxrTransactionRpcRequest,
            RpcRequestType.GATEWAY_STATUS: GatewayStatusRpcRequest,
            RpcRequestType.STOP: GatewayStopRpcRequest,
            RpcRequestType.MEMORY: GatewayMemoryRpcRequest,
            RpcRequestType.PEERS: GatewayPeersRpcRequest,
            RpcRequestType.BDN_PERFORMANCE: BdnPerformanceRpcRequest
        }
        self.request_id = ""
