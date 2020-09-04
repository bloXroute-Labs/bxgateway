import dataclasses
import time

from dataclasses import dataclass
from collections import deque
from typing import Type, Dict, Deque, Any, TYPE_CHECKING

from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants
from bxutils import logging, utils
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


@dataclass
class GatewayTransactionStatInterval(StatsIntervalData):
    new_transactions_received_from_blockchain: int = 0
    duplicate_transactions_received_from_blockchain: int = 0
    new_full_transactions_received_from_relays: int = 0
    new_compact_transactions_received_from_relays: int = 0
    duplicate_full_transactions_received_from_relays: int = 0
    duplicate_compact_transactions_received_from_relays: int = 0
    redundant_transaction_content_messages: int = 0
    short_id_assignments_processed: int = 0
    transaction_tracker: Dict[Sha256Hash, float] = dataclasses.field(default_factory=dict)
    transaction_intervals: Deque[float] = dataclasses.field(default_factory=deque)

    total_bdn_transactions_processed: int = 0
    total_bdn_transactions_process_time_ms: float = 0
    total_bdn_transactions_process_time_before_ext_ms: float = 0
    total_bdn_transactions_process_time_ext_ms: float = 0
    total_bdn_transactions_process_time_after_ext_ms: float = 0

    total_node_transactions_processed: int = 0
    total_node_transactions_process_time_ms: float = 0
    total_node_transactions_process_time_before_broadcast_ms: float = 0
    total_node_transactions_process_time_broadcast_ms: float = 0
    total_node_transactions_process_time_after_broadcast_ms: float = 0

    tx_validation_failed_gas_price: int = 0
    tx_validation_failed_structure: int = 0
    tx_validation_failed_signature: int = 0

    dropped_transactions_from_relay: int = 0

    transactions_bytes_skipped: int = 0


class _GatewayTransactionStatsService(
    StatisticsService[GatewayTransactionStatInterval, "AbstractGatewayNode"]
):
    TRANSACTION_SHORT_ID_ASSIGNED_DONE = -1

    def __init__(
        self,
        interval: int = gateway_constants.GATEWAY_TRANSACTION_STATS_INTERVAL_S,
        look_back: int = gateway_constants.GATEWAY_TRANSACTION_STATS_LOOKBACK,
    ) -> None:
        super(_GatewayTransactionStatsService, self).__init__(
            "GatewayTransactionStats",
            interval,
            look_back,
            reset=True,
            stat_logger=logging.get_logger(LogRecordType.TransactionStats, __name__),
        )

    def get_interval_data_class(self) -> Type[GatewayTransactionStatInterval]:
        return GatewayTransactionStatInterval

    def log_transaction_from_blockchain(self, transaction_hash: Sha256Hash) -> None:
        self.interval_data.new_transactions_received_from_blockchain += 1
        if transaction_hash not in self.interval_data.transaction_tracker:
            self.interval_data.transaction_tracker[transaction_hash] = time.time()

    def log_duplicate_transaction_from_blockchain(self) -> None:
        self.interval_data.duplicate_transactions_received_from_blockchain += 1

    def log_transaction_from_relay(
        self, transaction_hash: Sha256Hash, has_short_id: bool, is_compact: bool = False
    ) -> None:
        if has_short_id and transaction_hash in self.interval_data.transaction_tracker:
            start_time = self.interval_data.transaction_tracker[transaction_hash]
            if start_time != self.TRANSACTION_SHORT_ID_ASSIGNED_DONE:
                self.interval_data.transaction_intervals.append(time.time() - start_time)
                self.interval_data.transaction_tracker[
                    transaction_hash
                ] = self.TRANSACTION_SHORT_ID_ASSIGNED_DONE

        if is_compact:
            self.interval_data.new_compact_transactions_received_from_relays += 1
        else:
            self.interval_data.new_full_transactions_received_from_relays += 1

    def log_short_id_assignment_processed(self) -> None:
        self.interval_data.short_id_assignments_processed += 1

    def log_duplicate_transaction_from_relay(self, is_compact=False) -> None:
        if is_compact:
            self.interval_data.duplicate_compact_transactions_received_from_relays += 1
        else:
            self.interval_data.duplicate_full_transactions_received_from_relays += 1

    def log_dropped_transaction_from_relay(self) -> None:
        interval_data = self.interval_data
        interval_data.dropped_transactions_from_relay += 1

    def log_redundant_transaction_content(self) -> None:
        self.interval_data.redundant_transaction_content_messages += 1

    def log_processed_bdn_transaction(self,
                                      processing_time_ms: float,
                                      processing_time_before_ext_ms: float,
                                      processing_time_ext_ms: float,
                                      processing_time_after_ext_ms: float
                                      ) -> None:
        interval_data = self.interval_data
        interval_data.total_bdn_transactions_processed += 1
        interval_data.total_bdn_transactions_process_time_ms += processing_time_ms
        interval_data.total_bdn_transactions_process_time_before_ext_ms += processing_time_before_ext_ms
        interval_data.total_bdn_transactions_process_time_ext_ms += processing_time_ext_ms
        interval_data.total_bdn_transactions_process_time_after_ext_ms += processing_time_after_ext_ms

    def log_processed_node_transaction(self,
                                       total_duration_ms: float,
                                       duration_before_broadcast_ms: float,
                                       duration_broadcast_ms: float,
                                       duration_set_content_ms: float,
                                       count: int
                                       ) -> None:
        interval_data = self.interval_data
        interval_data.total_node_transactions_processed += count
        interval_data.total_node_transactions_process_time_ms += total_duration_ms
        interval_data.total_node_transactions_process_time_before_broadcast_ms += duration_before_broadcast_ms
        interval_data.total_node_transactions_process_time_broadcast_ms += duration_broadcast_ms
        interval_data.total_node_transactions_process_time_after_broadcast_ms += duration_set_content_ms

    def log_tx_validation_failed_signature(self,) -> None:
        self.interval_data.tx_validation_failed_signature += 1

    def log_tx_validation_failed_structure(self,) -> None:
        self.interval_data.tx_validation_failed_structure += 1

    def log_tx_validation_failed_gas_price(self,) -> None:
        self.interval_data.tx_validation_failed_gas_price += 1

    def log_skipped_transaction_bytes(self, skipped_bytes: int) -> None:
        self.interval_data.transactions_bytes_skipped += skipped_bytes

    def get_info(self) -> Dict[str, Any]:
        node = self.node
        assert node is not None

        if len(self.interval_data.transaction_intervals) > 0:
            min_short_id_assign_time = min(self.interval_data.transaction_intervals)
            max_short_id_assign_time = max(self.interval_data.transaction_intervals)
            avg_short_id_assign_time = sum(self.interval_data.transaction_intervals) / len(
                self.interval_data.transaction_intervals
            )
        else:
            min_short_id_assign_time = 0
            max_short_id_assign_time = 0
            avg_short_id_assign_time = 0

        interval_data = self.interval_data

        return {
            "node_id": node.opts.node_id,
            "new_transactions_received_from_blockchain": interval_data.new_transactions_received_from_blockchain,
            "duplicate_transactions_received_from_blockchain": interval_data.duplicate_transactions_received_from_blockchain,
            "new_full_transactions_received_from_relays": interval_data.new_full_transactions_received_from_relays,
            "new_compact_transactions_received_from_relays": interval_data.new_compact_transactions_received_from_relays,
            "duplicate_full_transactions_received_from_relays": interval_data.duplicate_full_transactions_received_from_relays,
            "duplicate_compact_transactions_received_from_relays": interval_data.duplicate_compact_transactions_received_from_relays,
            "redundant_transaction_content_messages": interval_data.redundant_transaction_content_messages,
            "dropped_transactions_from_relay": interval_data.dropped_transactions_from_relay,
            "short_ids_assignments_processed": interval_data.short_id_assignments_processed,
            "start_time": interval_data.start_time,
            "end_time": interval_data.end_time,
            "min_short_id_assign_time": min_short_id_assign_time,
            "max_short_id_assign_time": max_short_id_assign_time,
            "avg_short_id_assign_time": avg_short_id_assign_time,

            "total_bdn_transactions_processed": interval_data.total_bdn_transactions_processed,
            "total_bdn_transactions_process_time_ms": interval_data.total_bdn_transactions_process_time_ms,
            "average_bdn_tx_process_time_ms":
                utils.safe_divide(
                    interval_data.total_bdn_transactions_process_time_ms,
                    interval_data.total_bdn_transactions_processed),
            "average_bdn_tx_process_time_ext_ms":
                utils.safe_divide(
                    interval_data.total_bdn_transactions_process_time_ext_ms,
                    interval_data.total_bdn_transactions_processed),
            "average_bdn_tx_process_time_before_ext_ms":
                utils.safe_divide(
                    interval_data.total_bdn_transactions_process_time_before_ext_ms,
                    interval_data.total_bdn_transactions_processed),
            "average_bdn_tx_process_time_after_ext_ms":
                utils.safe_divide(
                    interval_data.total_bdn_transactions_process_time_after_ext_ms,
                    interval_data.total_bdn_transactions_processed),

            "total_node_transactions_processed": interval_data.total_node_transactions_processed,
            "total_node_transactions_process_time_ms": interval_data.total_node_transactions_process_time_ms,
            "average_node_tx_process_time_ms":
                utils.safe_divide(
                    interval_data.total_node_transactions_process_time_ms,
                    interval_data.total_node_transactions_processed),
            "average_node_tx_process_time_before_broadcast_ms":
                utils.safe_divide(
                    interval_data.total_node_transactions_process_time_before_broadcast_ms,
                    interval_data.total_node_transactions_processed),
            "average_node_tx_process_time_broadcast_ms":
                utils.safe_divide(
                    interval_data.total_node_transactions_process_time_broadcast_ms,
                    interval_data.total_node_transactions_processed),
            "average_node_tx_process_time_after_broadcast_ms":
                utils.safe_divide(
                    interval_data.total_node_transactions_process_time_after_broadcast_ms,
                    interval_data.total_node_transactions_processed),
            "rejected_signature": interval_data.tx_validation_failed_signature,
            "rejected_structure": interval_data.tx_validation_failed_structure,
            "rejected_gas_price": interval_data.tx_validation_failed_gas_price,
            "transaction_bytes_skipped": interval_data.transactions_bytes_skipped,
            **node._tx_service.get_aggregate_stats(),
        }


gateway_transaction_stats_service = _GatewayTransactionStatsService()
