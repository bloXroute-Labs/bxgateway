from typing import Type, Dict, Any, TYPE_CHECKING

from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class EthGatewayStatInterval(StatsIntervalData):
    total_encryption_time: float
    total_encrypted_msgs_count: int
    max_encryption_time: float
    total_decryption_time: float
    total_decrypted_msgs_count: int
    max_decryption_time: float
    total_serialization_time: float
    total_serialized_msgs_count: int
    max_serialization_time: float

    def __init__(self, *args, **kwargs):
        super(EthGatewayStatInterval, self).__init__(*args, **kwargs)
        self.total_encryption_time = 0
        self.total_encrypted_msgs_count = 0
        self.max_encryption_time = 0
        self.total_decryption_time = 0
        self.total_decrypted_msgs_count = 0
        self.max_decryption_time = 0
        self.total_serialization_time = 0
        self.total_serialized_msgs_count = 0
        self.max_serialization_time = 0


class _EthGatewayStatsService(StatisticsService[EthGatewayStatInterval, "AbstractGatewayNode"]):
    def __init__(
        self,
        interval: int = gateway_constants.ETH_GATEWAY_STATS_INTERVAL,
        look_back: int = gateway_constants.ETH_GATEWAY_STATS_LOOKBACK,
    ):
        super(_EthGatewayStatsService, self).__init__(
            "EthGatewayStatsService",
            interval,
            look_back,
            reset=True,
            stat_logger=logging.get_logger(LogRecordType.TransactionStats, __name__),
        )

    def get_interval_data_class(self) -> Type[EthGatewayStatInterval]:
        return EthGatewayStatInterval

    def log_encrypted_message(self, time: float) -> None:
        assert self.interval_data is not None
        # pyre-fixme[16]: Optional type has no attribute `total_encryption_time`.
        self.interval_data.total_encryption_time += time
        # pyre-fixme[16]: Optional type has no attribute `total_encrypted_msgs_count`.
        self.interval_data.total_encrypted_msgs_count += 1
        # pyre-fixme[16]: Optional type has no attribute `max_encryption_time`.
        self.interval_data.max_encryption_time = max(self.interval_data.max_encryption_time, time)

    def log_decrypted_message(self, time: float) -> None:
        assert self.interval_data is not None
        # pyre-fixme[16]: Optional type has no attribute `total_decryption_time`.
        self.interval_data.total_decryption_time += time
        # pyre-fixme[16]: Optional type has no attribute `total_decrypted_msgs_count`.
        self.interval_data.total_decrypted_msgs_count += 1
        # pyre-fixme[16]: Optional type has no attribute `max_decryption_time`.
        self.interval_data.max_decryption_time = max(self.interval_data.max_decryption_time, time)

    def log_serialized_message(self, time: float) -> None:
        assert self.interval_data is not None
        # pyre-fixme[16]: Optional type has no attribute `total_serialization_time`.
        self.interval_data.total_serialization_time += time
        # pyre-fixme[16]: Optional type has no attribute `total_serialized_msgs_count`.
        self.interval_data.total_serialized_msgs_count += 1
        # pyre-fixme[16]: Optional type has no attribute `max_serialization_time`.
        self.interval_data.max_serialization_time = max(
            self.interval_data.max_serialization_time, time
        )

    def get_info(self) -> Dict[str, Any]:
        assert self.interval_data is not None
        # pyre-fixme[16]: Optional type has no attribute `total_encryption_time`.
        if self.interval_data.total_encryption_time > 0:
            average_encryption_time = (
                self.interval_data.total_encryption_time
                / self.interval_data.total_encrypted_msgs_count
            )
        else:
            average_encryption_time = 0

        # pyre-fixme[16]: Optional type has no attribute `total_decryption_time`.
        if self.interval_data.total_decryption_time > 0:
            average_decryption_time = (
                self.interval_data.total_decryption_time
                / self.interval_data.total_decrypted_msgs_count
            )
        else:
            average_decryption_time = 0

        # pyre-fixme[16]: Optional type has no attribute `total_serialization_time`.
        if self.interval_data.total_serialization_time > 0:
            average_serialization_time = (
                self.interval_data.total_serialization_time
                / self.interval_data.total_serialized_msgs_count
            )
        else:
            average_serialization_time = 0

        return {
            # pyre-fixme[16]: Optional type has no attribute
            #  `total_encrypted_msgs_count`.
            "total_encrypted_msgs_count": self.interval_data.total_encrypted_msgs_count,
            "average_encryption_time": stats_format.duration(average_encryption_time * 1000),
            "max_encryption_time": stats_format.duration(
                # pyre-fixme[16]: Optional type has no attribute `max_encryption_time`.
                self.interval_data.max_encryption_time * 1000
            ),
            # pyre-fixme[16]: Optional type has no attribute
            #  `total_decrypted_msgs_count`.
            "total_decrypted_msgs_count": self.interval_data.total_decrypted_msgs_count,
            "average_decryption_time": stats_format.duration(average_decryption_time * 1000),
            "max_decryption_time": stats_format.duration(
                # pyre-fixme[16]: Optional type has no attribute `max_decryption_time`.
                self.interval_data.max_decryption_time * 1000
            ),
            # pyre-fixme[16]: Optional type has no attribute
            #  `total_serialized_msgs_count`.
            "total_serialized_msgs_count": self.interval_data.total_serialized_msgs_count,
            "average_serialization_time": stats_format.duration(average_serialization_time * 1000),
            "max_serialization_time": stats_format.duration(
                # pyre-fixme[16]: Optional type has no attribute
                #  `max_serialization_time`.
                self.interval_data.max_serialization_time * 1000
            ),
        }


eth_gateway_stats_service = _EthGatewayStatsService()
