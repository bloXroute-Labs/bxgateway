from typing import Dict, Any, TYPE_CHECKING, List, Type

from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxcommon.rpc import rpc_constants
from bxgateway import eth_constants


from bxutils import logging
from bxutils.logging import LogRecordType


if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class EthOnBlockFeedStatInterval(StatsIntervalData):
    subscriber_task_count: List[int]
    subscriber_task_duration: List[float]

    def __init__(self):
        super().__init__()
        self.subscriber_task_count = [0]
        self.subscriber_task_duration = [0]


class EthOnBlockFeedStatsService(
    StatisticsService[EthOnBlockFeedStatInterval, "AbstractGatewayNode"]
):
    def __init__(
        self,
        interval: int = eth_constants.ETH_ON_BLOCK_FEED_STATS_INTERVAL_S,
        look_back: int = eth_constants.ETH_ON_BLOCK_FEED_STATS_LOOKBACK,
    ) -> None:
        super().__init__(
            "EthCallFeedStats",
            interval,
            look_back,
            reset=True,
            stat_logger=logging.get_logger(LogRecordType.EthOnBlockFeedStats),
        )
        self.feed_name = rpc_constants.ETH_ON_BLOCK_FEED_NAME

    def get_interval_data_class(self) -> Type[EthOnBlockFeedStatInterval]:
        return EthOnBlockFeedStatInterval

    def get_info(self) -> Dict[str, Any]:
        interval_data = self.interval_data

        node = self.node
        assert node is not None
        feeds = node.feed_manager.feeds
        subscriber_count = 0
        if feeds and self.feed_name in feeds:
            feed = feeds[self.feed_name]
            subscriber_count = len(feed.subscribers)

        return {
            "subscriber_count": subscriber_count,
            "total_calls": sum(interval_data.subscriber_task_count),
            "max_calls_per_subscriber": max(interval_data.subscriber_task_count, default=None),
            "max_duration": max(interval_data.subscriber_task_duration, default=None),
        }

    def log_subscriber_tasks(self, tasks_count: int, duration_s: float) -> None:
        interval_data = self.interval_data
        interval_data.subscriber_task_count.append(tasks_count)
        interval_data.subscriber_task_duration.append(duration_s)


eth_on_block_feed_stats_service = EthOnBlockFeedStatsService()
