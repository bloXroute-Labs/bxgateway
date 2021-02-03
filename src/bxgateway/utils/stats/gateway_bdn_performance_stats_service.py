from dataclasses import dataclass
from typing import Type, Dict, Any, Optional, TYPE_CHECKING

from prometheus_client import Counter

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.bloxroute.bdn_performance_stats_message import BdnPerformanceStatsData
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


@dataclass
class GatewayBdnPerformanceStatInterval(StatsIntervalData):
    blockchain_node_to_bdn_stats: Optional[Dict[IpEndpoint, BdnPerformanceStatsData]] = None

    def __init__(self):
        super().__init__()
        self.blockchain_node_to_bdn_stats = {}


blocks_from_bdn = Counter(
    "blocks_from_bdn", "Number of blocks received first from the BDN", ['ip_endpoint']
)
blocks_from_blockchain = Counter(
    "blocks_from_blockchain", "Number of blocks received first from the blockchain node", ['ip_endpoint']
)
blocks_seen = Counter(
    "blocks_seen", "Total number of unique new block seen by gateway", ['ip_endpoint']
)
transactions_from_bdn = Counter(
    "transactions_from_bdn", "Number of transactions received first from the BDN", ['ip_endpoint']
)
transactions_from_blockchain = Counter(
    "transactions_from_blockchain", "Number of transactions received first from the blockchain node", ['ip_endpoint']
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

    def create_interval_data_object(self) -> None:
        super().create_interval_data_object()

        node = self.node
        assert node is not None
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None

        active_blockchain_peers = list(node.connection_pool.get_by_connection_types((ConnectionType.BLOCKCHAIN_NODE,)))
        for blockchain_peer in active_blockchain_peers:
            blockchain_node_to_bdn_stats[blockchain_peer.endpoint] = BdnPerformanceStatsData()

    def get_node_stats(
        self, node_endpoint: IpEndpoint, blockchain_node_to_bdn_stats: Dict[IpEndpoint, BdnPerformanceStatsData]
    ) -> BdnPerformanceStatsData:
        if node_endpoint in blockchain_node_to_bdn_stats:
            node_stats = blockchain_node_to_bdn_stats[node_endpoint]
        else:
            blockchain_node_to_bdn_stats[node_endpoint] = BdnPerformanceStatsData()
            node_stats = blockchain_node_to_bdn_stats[node_endpoint]
        return node_stats

    def get_interval_data_class(self) -> Type[GatewayBdnPerformanceStatInterval]:
        return GatewayBdnPerformanceStatInterval

    def get_info(self) -> Dict[str, Any]:
        return {}

    def log_block_from_blockchain_node(self, node_endpoint: IpEndpoint) -> None:
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None
        node_stats = self.get_node_stats(node_endpoint, blockchain_node_to_bdn_stats)

        node_stats.new_blocks_received_from_blockchain_node += 1
        node_stats.new_blocks_seen += 1
        blocks_from_blockchain.labels(f"{node_endpoint.ip_address}:{node_endpoint.port}").inc()
        blocks_seen.labels(f"{node_endpoint.ip_address}:{node_endpoint.port}").inc()

        for endpoint, stats in blockchain_node_to_bdn_stats.items():
            if endpoint == node_endpoint:
                continue
            stats.new_blocks_received_from_bdn += 1
            stats.new_blocks_seen += 1
            blocks_from_bdn.labels(f"{endpoint.ip_address}:{endpoint.port}").inc()
            blocks_seen.labels(f"{endpoint.ip_address}:{endpoint.port}").inc()

    def log_block_message_from_blockchain_node(self, node_endpoint: IpEndpoint, is_full_block: bool) -> None:
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None
        node_stats = self.get_node_stats(node_endpoint, blockchain_node_to_bdn_stats)

        if is_full_block:
            node_stats.new_block_messages_from_blockchain_node += 1
        else:
            node_stats.new_block_announcements_from_blockchain_node += 1

    def log_block_from_bdn(self) -> None:
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None

        for endpoint, stats in blockchain_node_to_bdn_stats.items():
            stats.new_blocks_received_from_bdn += 1
            stats.new_blocks_seen += 1
            blocks_from_bdn.labels(f"{endpoint.ip_address}:{endpoint.port}").inc()
            blocks_seen.labels(f"{endpoint.ip_address}:{endpoint.port}").inc()

    def log_tx_from_blockchain_node(self, node_endpoint: IpEndpoint, low_fee: bool = False) -> None:
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None
        node_stats = self.get_node_stats(node_endpoint, blockchain_node_to_bdn_stats)

        transactions_from_blockchain.labels(f"{node_endpoint.ip_address}:{node_endpoint.port}").inc()

        if low_fee:
            node_stats.new_tx_received_from_blockchain_node_low_fee += 1
        else:
            node_stats.new_tx_received_from_blockchain_node += 1
            for endpoint, stats in blockchain_node_to_bdn_stats.items():
                if endpoint == node_endpoint:
                    continue
                stats.new_tx_received_from_bdn += 1
                transactions_from_bdn.labels(f"{endpoint.ip_address}:{endpoint.port}").inc()

    def log_tx_from_bdn(self, low_fee: bool = False) -> None:
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None

        for endpoint, stats in blockchain_node_to_bdn_stats.items():
            transactions_from_bdn.labels(f"{endpoint.ip_address}:{endpoint.port}").inc()
            if low_fee:
                stats.new_tx_received_from_bdn_low_fee += 1
            else:
                stats.new_tx_received_from_bdn += 1

    def log_tx_sent_to_nodes(self, broadcasting_endpoint: Optional[IpEndpoint] = None):
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None

        for endpoint, stats in blockchain_node_to_bdn_stats.items():
            if broadcasting_endpoint is not None and endpoint == broadcasting_endpoint:
                continue
            stats.tx_sent_to_node += 1

    def log_duplicate_tx_from_node(self, node_endpoint: IpEndpoint):
        blockchain_node_to_bdn_stats = self.interval_data.blockchain_node_to_bdn_stats
        assert blockchain_node_to_bdn_stats is not None
        node_stats = self.get_node_stats(node_endpoint, blockchain_node_to_bdn_stats)
        node_stats.duplicate_tx_from_node += 1

    def get_most_recent_stats(self) -> Optional[GatewayBdnPerformanceStatInterval]:
        if self.history:
            interval_data = self.history[0]
            return interval_data
        else:
            return None


gateway_bdn_performance_stats_service = _GatewayBdnPerformanceStatsService()
