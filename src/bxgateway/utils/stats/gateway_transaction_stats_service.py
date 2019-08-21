import time
from collections import deque

from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants


class GatewayTransactionStatInterval(StatsIntervalData):
    __slots__ = [
        "new_transactions_received_from_blockchain",
        "new_transactions_received_from_relays",
        "compact_transactions_received_from_relays",
        "duplicate_transactions_received_from_blockchain",
        "duplicate_transactions_received_from_relays",
        "short_id_assignments_processed",
        "redundant_transaction_content_messages",
        "transaction_tracker",
        "transaction_intervals"
    ]

    def __init__(self, *args, **kwargs):
        super(GatewayTransactionStatInterval, self).__init__(*args, **kwargs)
        self.new_transactions_received_from_blockchain = 0
        self.new_transactions_received_from_relays = 0
        self.compact_transactions_received_from_relays = 0
        self.duplicate_transactions_received_from_blockchain = 0
        self.duplicate_transactions_received_from_relays = 0
        self.short_id_assignments_processed = 0
        self.redundant_transaction_content_messages = 0
        self.transaction_tracker = {}
        self.transaction_intervals = deque()


class _GatewayTransactionStatsService(StatisticsService):
    INTERVAL_DATA_CLASS = GatewayTransactionStatInterval
    TRANSACTION_SHORT_ID_ASSIGNED_DONE = -1

    def __init__(self, interval=gateway_constants.GATEWAY_TRANSACTION_STATS_INTERVAL_S,
                 look_back=gateway_constants.GATEWAY_TRANSACTION_STATS_LOOKBACK):
        super(_GatewayTransactionStatsService, self).__init__("GatewayTransactionStats", interval, look_back,
                                                              reset=True)

    def log_transaction_from_blockchain(self, transaction_hash):
        self.interval_data.new_transactions_received_from_blockchain += 1
        if transaction_hash not in self.interval_data.transaction_tracker:
            self.interval_data.transaction_tracker[transaction_hash] = time.time()

    def log_duplicate_transaction_from_blockchain(self):
        self.interval_data.duplicate_transactions_received_from_blockchain += 1

    def log_transaction_from_relay(self, transaction_hash, has_short_id, is_compact=False):
        self.interval_data.new_transactions_received_from_relays += 1
        if has_short_id and transaction_hash in self.interval_data.transaction_tracker:
            start_time = self.interval_data.transaction_tracker[transaction_hash]
            if start_time != self.TRANSACTION_SHORT_ID_ASSIGNED_DONE:
                self.interval_data.transaction_intervals.append(time.time() - start_time)
                self.interval_data.transaction_tracker[transaction_hash] = self.TRANSACTION_SHORT_ID_ASSIGNED_DONE

        if has_short_id:
            self.interval_data.short_id_assignments_processed += 1
        if is_compact:
            self.interval_data.compact_transactions_received_from_relays += 1

    def log_duplicate_transaction_from_relay(self):
        self.interval_data.duplicate_transactions_received_from_relays += 1

    def log_redundant_transaction_content(self):
        self.interval_data.redundant_transaction_content_messages += 1

    def get_info(self):
        if len(self.interval_data.transaction_intervals) > 0:
            min_short_id_assign_time = min(self.interval_data.transaction_intervals)
            max_short_id_assign_time = max(self.interval_data.transaction_intervals)
            avg_short_id_assign_time = sum(self.interval_data.transaction_intervals) / \
                                       len(self.interval_data.transaction_intervals)
        else:
            min_short_id_assign_time = 0
            max_short_id_assign_time = 0
            avg_short_id_assign_time = 0

        return dict(
            node_id=self.interval_data.node_id,
            transactions_received_from_blockchain=self.interval_data.new_transactions_received_from_blockchain,
            transactions_received_from_relays=self.interval_data.new_transactions_received_from_relays,
            compact_transactions_received_from_relays=self.interval_data.compact_transactions_received_from_relays,
            duplicate_transactions_received_from_blockchain=self.interval_data.duplicate_transactions_received_from_blockchain,
            duplicate_transactions_received_from_relays=self.interval_data.duplicate_transactions_received_from_relays,
            short_ids_assignments_processed=self.interval_data.short_id_assignments_processed,
            redundant_transaction_content_messages=self.interval_data.redundant_transaction_content_messages,
            start_time=self.interval_data.start_time,
            end_time=self.interval_data.end_time,
            min_short_id_assign_time=min_short_id_assign_time,
            max_short_id_assign_time=max_short_id_assign_time,
            avg_short_id_assign_time=avg_short_id_assign_time,
            **self.node._tx_service.get_tx_service_aggregate_stats()
        )


gateway_transaction_stats_service = _GatewayTransactionStatsService()
