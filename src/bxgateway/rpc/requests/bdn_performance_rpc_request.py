from typing import Any, Dict, Optional

from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk

from bxcommon.utils.stats import stats_format
from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractGatewayRpcRequest
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import (
    gateway_bdn_performance_stats_service,
    GatewayBdnPerformanceStatInterval,
)

BLOCKS_FROM_BDN = "blocks_from_bdn_percentage"
TX_FROM_BDN = "transactions_from_bdn_percentage"


class BdnPerformanceRpcRequest(AbstractGatewayRpcRequest):
    help = {
        "params": "",
        "description": "return percentage of blocks/transactions first received from BDN rather than from p2p network"
        "in the previous 15 minute interval",
    }

    async def process_request(self) -> Response:
        return self._format_response(self._calc_bdn_performance_stats(), HTTPOk)

    def _calc_bdn_performance_stats(self) -> Dict[str, Any]:
        stats = {}
        interval_data: Optional[
            GatewayBdnPerformanceStatInterval
        ] = gateway_bdn_performance_stats_service.get_most_recent_stats()

        if interval_data is None:
            stats[BLOCKS_FROM_BDN] = float("nan")
            stats[TX_FROM_BDN] = float("nan")
            stats["interval_start_time"] = float("nan")
            stats["interval_end_time"] = float("nan")
            return stats

        stats["interval_start_time"] = str(interval_data.start_time)
        assert interval_data.end_time is not None
        stats["interval_end_time"] = str(interval_data.end_time)

        if (
            interval_data.new_blocks_received_from_bdn
            + interval_data.new_blocks_received_from_blockchain_node
            == 0
        ):
            stats[BLOCKS_FROM_BDN] = float("nan")
        if (
            interval_data.new_tx_received_from_bdn
            + interval_data.new_tx_received_from_blockchain_node
            == 0
        ):
            stats[TX_FROM_BDN] = float("nan")

        if BLOCKS_FROM_BDN not in stats:
            stats[BLOCKS_FROM_BDN] = stats_format.percentage(
                100
                * (
                    interval_data.new_blocks_received_from_bdn
                    / (
                        interval_data.new_blocks_received_from_bdn
                        + interval_data.new_blocks_received_from_blockchain_node
                    )
                )
            )
        if TX_FROM_BDN not in stats:
            stats[TX_FROM_BDN] = stats_format.percentage(
                100
                * (
                    interval_data.new_tx_received_from_bdn
                    / (
                        interval_data.new_tx_received_from_bdn
                        + interval_data.new_tx_received_from_blockchain_node
                    )
                )
            )

        return stats
