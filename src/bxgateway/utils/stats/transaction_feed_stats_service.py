import time
import dataclasses

from dataclasses import dataclass
from typing import Dict, Any, TYPE_CHECKING, List, Type

from bxcommon.feed.feed import FeedKey
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants
from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxutils import logging
from bxutils.logging import LogRecordType
from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


@dataclass
class TransactionFeedStatInterval(StatsIntervalData):
    new_transaction_received_times: Dict[Sha256Hash, float] = dataclasses.field(default_factory=dict)
    # lower time of the two below
    pending_transaction_received_times: Dict[Sha256Hash, float] = dataclasses.field(default_factory=dict)
    pending_transaction_from_internal_received_times: Dict[Sha256Hash, float] = dataclasses.field(default_factory=dict)
    pending_transaction_from_local_blockchain_received_times: Dict[Sha256Hash, float] = \
        dataclasses.field(default_factory=dict)

    new_transactions_faster_than_pending_times: List[float] = dataclasses.field(default_factory=list)
    pending_from_internal_faster_than_local_times: List[float] = dataclasses.field(default_factory=list)

    pending_transactions_missing_contents: int = 0


class TransactionFeedStatsService(
    StatisticsService[TransactionFeedStatInterval, "AbstractGatewayNode"]
):

    def __init__(
        self,
        interval: int = gateway_constants.GATEWAY_TRANSACTION_FEED_STATS_INTERVAL_S,
        look_back: int = gateway_constants.GATEWAY_TRANSACTION_FEED_STATS_LOOKBACK
    ) -> None:
        super().__init__(
            "TransactionFeedStats",
            interval,
            look_back,
            reset=True,
            stat_logger=logging.get_logger(LogRecordType.TransactionFeedStats)
        )

    def get_interval_data_class(self) -> Type[TransactionFeedStatInterval]:
        return TransactionFeedStatInterval

    def get_info(self) -> Dict[str, Any]:
        interval_data = self.interval_data

        if len(interval_data.new_transactions_faster_than_pending_times) > 0:
            avg_new_transactions_faster_by_s = (
                sum(interval_data.new_transactions_faster_than_pending_times)
                / len(interval_data.new_transactions_faster_than_pending_times)
            )
        else:
            avg_new_transactions_faster_by_s = 0
        new_transactions_faster_count = len([
            diff for diff in interval_data.new_transactions_faster_than_pending_times
            if diff > 0
        ])

        if len(interval_data.pending_from_internal_faster_than_local_times) > 0:
            avg_pending_transactions_from_internal_faster_by_s = (
                sum(interval_data.pending_from_internal_faster_than_local_times)
                / len(interval_data.pending_from_internal_faster_than_local_times)
            )
        else:
            avg_pending_transactions_from_internal_faster_by_s = 0
        pending_transactions_from_internal_faster_count = len([
            diff for diff in interval_data.pending_from_internal_faster_than_local_times
            if diff > 0
        ])

        node = self.node
        assert node is not None
        feeds = node.feed_manager.feeds

        pending_transaction_feed = node.feed_manager.get_feed(FeedKey(EthPendingTransactionFeed.NAME))
        if pending_transaction_feed:
            pending_transaction_feed_subscribers = len(pending_transaction_feed.subscribers)
        else:
            pending_transaction_feed_subscribers = None
        new_transaction_feed = node.feed_manager.get_feed(FeedKey(EthNewTransactionFeed.NAME))
        if new_transaction_feed:
            new_transaction_feed_subscribers = len(new_transaction_feed.subscribers)
        else:
            new_transaction_feed_subscribers = None

        return {
            "start_time": self.interval_data.start_time,
            "end_time": self.interval_data.end_time,
            "new_transactions": len(interval_data.new_transaction_received_times),
            "pending_transactions": len(interval_data.pending_transaction_received_times),
            "pending_transactions_from_internal": len(
                interval_data.pending_transaction_from_internal_received_times
            ),
            "pending_transactions_from_local": len(
                interval_data.pending_transaction_from_local_blockchain_received_times
            ),
            "avg_new_transactions_faster_by_s": avg_new_transactions_faster_by_s,
            "new_transactions_faster_count": new_transactions_faster_count,
            "avg_pending_transactions_from_internal_faster_by_s": avg_pending_transactions_from_internal_faster_by_s,
            "pending_transactions_from_internal_faster_count": pending_transactions_from_internal_faster_count,
            "pending_transactions_missing_contents": interval_data.pending_transactions_missing_contents,
            "pending_transaction_feed_subscribers": pending_transaction_feed_subscribers,
            "new_transaction_feed_subscribers": new_transaction_feed_subscribers,
            "gateway_transaction_streamers": node.get_gateway_transaction_streamers_count()
        }

    def log_new_transaction(self, tx_hash: Sha256Hash) -> None:
        interval_data = self.interval_data
        current_time = time.time()

        interval_data.new_transaction_received_times[tx_hash] = current_time
        if tx_hash in interval_data.pending_transaction_received_times:
            timestamp = interval_data.pending_transaction_received_times[tx_hash]
            interval_data.new_transactions_faster_than_pending_times.append(
                timestamp - current_time
            )

    def log_pending_transaction_from_internal(self, tx_hash: Sha256Hash) -> None:
        interval_data = self.interval_data

        if tx_hash in interval_data.pending_transaction_from_internal_received_times:
            return

        current_time = time.time()

        interval_data.pending_transaction_from_internal_received_times[tx_hash] = current_time
        if tx_hash in interval_data.pending_transaction_received_times:
            # already received from local blockchain
            timestamp = interval_data.pending_transaction_from_local_blockchain_received_times[
                tx_hash
            ]
            interval_data.pending_from_internal_faster_than_local_times.append(
                timestamp - current_time
            )
        else:
            interval_data.pending_transaction_received_times[tx_hash] = current_time
            if tx_hash in interval_data.new_transaction_received_times:
                timestamp = interval_data.new_transaction_received_times[tx_hash]
                interval_data.new_transactions_faster_than_pending_times.append(
                    current_time - timestamp
                )

    def log_pending_transaction_from_local(self, tx_hash: Sha256Hash) -> None:
        interval_data = self.interval_data

        if tx_hash in interval_data.pending_transaction_from_local_blockchain_received_times:
            return

        current_time = time.time()

        interval_data.pending_transaction_from_local_blockchain_received_times[tx_hash] = current_time
        if tx_hash in interval_data.pending_transaction_from_internal_received_times:
            # already received from internal
            timestamp = interval_data.pending_transaction_from_internal_received_times[tx_hash]
            interval_data.pending_from_internal_faster_than_local_times.append(current_time - timestamp)
        else:
            interval_data.pending_transaction_received_times[tx_hash] = current_time
            if tx_hash in interval_data.new_transaction_received_times:
                timestamp = interval_data.new_transaction_received_times[tx_hash]
                interval_data.new_transactions_faster_than_pending_times.append(current_time - timestamp)

    def log_pending_transaction_missing_contents(self) -> None:
        self.interval_data.pending_transactions_missing_contents += 1


transaction_feed_stats_service = TransactionFeedStatsService()
