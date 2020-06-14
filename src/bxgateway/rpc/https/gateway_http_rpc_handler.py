from typing import TYPE_CHECKING

from bxcommon.rpc.https.http_rpc_handler import HttpRpcHandler
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxgateway.rpc.requests.bdn_performance_rpc_request import BdnPerformanceRpcRequest
from bxgateway.rpc.requests.gateway_blxr_transaction_rpc_request import GatewayBlxrTransactionRpcRequest
from bxgateway.rpc.requests.gateway_status_rpc_request import GatewayStatusRpcRequest
from bxgateway.rpc.requests.gateway_stop_rpc_request import GatewayStopRpcRequest
from bxgateway.rpc.requests.gateway_memory_rpc_request import GatewayMemoryRpcRequest
from bxgateway.rpc.requests.gateway_peers_rpc_request import GatewayPeersRpcRequest
from bxgateway.rpc.requests.quota_usage_rpc_request import QuotaUsageRpcRequest

from bxutils import logging


if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class GatewayHttpRpcHandler(HttpRpcHandler["AbstractGatewayNode"]):

    def __init__(self, node: "AbstractGatewayNode") -> None:
        super().__init__(node)
        self.request_handlers = {
            RpcRequestType.BLXR_TX: GatewayBlxrTransactionRpcRequest,
            RpcRequestType.GATEWAY_STATUS: GatewayStatusRpcRequest,
            RpcRequestType.STOP: GatewayStopRpcRequest,
            RpcRequestType.MEMORY: GatewayMemoryRpcRequest,
            RpcRequestType.PEERS: GatewayPeersRpcRequest,
            RpcRequestType.BDN_PERFORMANCE: BdnPerformanceRpcRequest,
            RpcRequestType.QUOTA_USAGE: QuotaUsageRpcRequest
        }
