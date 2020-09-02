from dataclasses import dataclass
from typing import Type, Dict, Any, Optional, TYPE_CHECKING

from prometheus_client import Counter

from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


@dataclass
class GatewayBdnPerformanceStatInterval(StatsIntervalData):
    new_blocks_received_from_blockchain_node: int = 0
    new_blocks_received_from_bdn: int = 0
    new_tx_received_from_blockchain_node: int = 0
    new_tx_received_from_bdn: int = 0
    new_tx_received_from_blockchain_node_low_fee: int = 0
    new_tx_received_from_bdn_low_fee: int = 0


blocks_from_bdn = Counter("blocks_from_bdn", "Number of blocks received first from the BDN")
blocks_from_blockchain = Counter("blocks_from_blockchain", "Number of blocks received first from the blockchain node")
transactions_from_bdn = Counter("transactions_from_bdn", "Number of transactions received first from the BDN")
transactions_from_blockchain = Counter(
    "transactions_from_blockchain", "Number of transactions received first from the blockchain node"
)


class _GatewayBdnPerformanceStatsService(
    StatisticsService[GatewayBdnPerformanceStatInterval, "AbstractGatewayNode"]
):
    def __init__(
        self,
        interval=gateway_constants.GATEWAY_BDN_PERFORMANCE_STATS_INTERVAL_S,
        look_back=gateway_constants.GATEWAY_BDN_PERFORMANCE_STATS_LOOKBACK,
    ) -> None:
        super(_GatewayBdnPerformanceStatsService, self).__init__(
            "GatewayBdnPerformanceStats",
            interval,
            look_back,
            reset=True,
            stat_logger=logging.get_logger(LogRecordType.BdnPerformanceStats, __name__),
        )

    def get_interval_data_class(self) -> Type[GatewayBdnPerformanceStatInterval]:
        return GatewayBdnPerformanceStatInterval

    def get_info(self) -> Dict[str, Any]:
        return {}

    def log_block_from_blockchain_node(self) -> None:
        self.interval_data.new_blocks_received_from_blockchain_node += 1
        blocks_from_blockchain.inc()

    def log_block_from_bdn(self) -> None:
        self.interval_data.new_blocks_received_from_bdn += 1
        blocks_from_bdn.inc()

    def log_tx_from_blockchain_node(self, low_fee: bool = False) -> None:
        transactions_from_blockchain.inc()
        if low_fee:
            self.interval_data.new_tx_received_from_blockchain_node_low_fee += 1
        else:
            self.interval_data.new_tx_received_from_blockchain_node += 1

    def log_tx_from_bdn(self, low_fee: bool = False) -> None:
        transactions_from_bdn.inc()
        if low_fee:
            self.interval_data.new_tx_received_from_bdn_low_fee += 1
        else:
            self.interval_data.new_tx_received_from_bdn += 1

    def get_most_recent_stats(self) -> Optional[GatewayBdnPerformanceStatInterval]:
        if self.history:
            interval_data = self.history[0]
            return interval_data
        else:
            return None


gateway_bdn_performance_stats_service = _GatewayBdnPerformanceStatsService()
