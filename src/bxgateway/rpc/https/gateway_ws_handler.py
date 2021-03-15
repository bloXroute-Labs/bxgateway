from typing import TYPE_CHECKING, cast

from bxcommon.feed.feed_manager import FeedManager
from bxcommon.rpc.abstract_ws_rpc_handler import AbstractWsRpcHandler
from bxcommon.rpc.requests.transaction_status_rpc_request import TransactionStatusRpcRequest
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxgateway.rpc.requests.add_blockchain_peer_rpc_request import AddBlockchainPeerRpcRequest
from bxgateway.rpc.requests.bdn_performance_rpc_request import BdnPerformanceRpcRequest
from bxgateway.rpc.requests.gateway_blxr_transaction_rpc_request import GatewayBlxrTransactionRpcRequest
from bxgateway.rpc.requests.gateway_memory_usage_report_rpc_request import GatewayMemoryUsageRpcRequest
from bxgateway.rpc.requests.gateway_status_rpc_request import GatewayStatusRpcRequest
from bxgateway.rpc.requests.gateway_stop_rpc_request import GatewayStopRpcRequest
from bxgateway.rpc.requests.gateway_memory_rpc_request import GatewayMemoryRpcRequest
from bxgateway.rpc.requests.gateway_peers_rpc_request import GatewayPeersRpcRequest
from bxgateway.rpc.requests.gateway_transaction_service_rpc_request import GatewayTransactionServiceRpcRequest
from bxgateway.rpc.requests.quota_usage_rpc_request import QuotaUsageRpcRequest
from bxgateway.rpc.requests.gateway_blxr_call_rpc_request import GatewayBlxrCallRpcRequest
from bxgateway.rpc.requests.remove_blockchain_peer_rpc_request import RemoveBlockchainPeerRpcRequest
from bxcommon.rpc.requests.subscribe_rpc_request import SubscribeRpcRequest
from bxcommon.rpc.requests.unsubscribe_rpc_request import UnsubscribeRpcRequest

from bxutils import logging
from bxutils.encoding.json_encoder import Case

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
    from bxcommon.connections.abstract_node import AbstractNode

logger = logging.get_logger(__name__)


class GatewayWsHandler(AbstractWsRpcHandler):

    def __init__(self, node: "AbstractGatewayNode", feed_manager: FeedManager, case: Case) -> None:
        super().__init__(node, feed_manager, case)
        self.request_handlers = {
            RpcRequestType.BLXR_TX: GatewayBlxrTransactionRpcRequest,
            RpcRequestType.BLXR_ETH_CALL: GatewayBlxrCallRpcRequest,
            RpcRequestType.GATEWAY_STATUS: GatewayStatusRpcRequest,
            RpcRequestType.STOP: GatewayStopRpcRequest,
            RpcRequestType.MEMORY: GatewayMemoryRpcRequest,
            RpcRequestType.PEERS: GatewayPeersRpcRequest,
            RpcRequestType.BDN_PERFORMANCE: BdnPerformanceRpcRequest,
            RpcRequestType.QUOTA_USAGE: QuotaUsageRpcRequest,
            RpcRequestType.MEMORY_USAGE: GatewayMemoryUsageRpcRequest,
            RpcRequestType.TX_STATUS: TransactionStatusRpcRequest,
            RpcRequestType.TX_SERVICE: GatewayTransactionServiceRpcRequest,
            RpcRequestType.ADD_BLOCKCHAIN_PEER: AddBlockchainPeerRpcRequest,
            RpcRequestType.REMOVE_BLOCKCHAIN_PEER: RemoveBlockchainPeerRpcRequest,
            RpcRequestType.SUBSCRIBE: SubscribeRpcRequest,
            RpcRequestType.UNSUBSCRIBE: UnsubscribeRpcRequest,
        }
