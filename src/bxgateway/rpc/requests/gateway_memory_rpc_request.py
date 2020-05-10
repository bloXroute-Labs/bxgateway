from typing import Dict, Any
from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk

from bxcommon.utils import memory_utils
from bxcommon.utils.stats import stats_format

from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractGatewayRpcRequest


TOTAL_MEM_USAGE = "total_mem_usage"
TOTAL_CACHED_TX = "total_cached_transactions"
TOTAL_CACHED_TX_SIZE = "total_cached_transactions_size"


class GatewayMemoryRpcRequest(AbstractGatewayRpcRequest):

    help = {
        "params": "",
        "description": "return gateway node memory information"
    }

    async def process_request(self) -> Response:
        return self._format_response(self._get_mem_stats(), HTTPOk)

    def _get_mem_stats(self) -> Dict[str, Any]:
        tx_service = self._node.get_tx_service()
        cache_state = tx_service.get_cache_state_json()
        return {
            TOTAL_MEM_USAGE: stats_format.byte_count(memory_utils.get_app_memory_usage()),
            TOTAL_CACHED_TX: cache_state["tx_hash_to_contents_len"],
            TOTAL_CACHED_TX_SIZE: stats_format.byte_count(cache_state["total_tx_contents_size"])
        }
