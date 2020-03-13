from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.statistics_service import StatisticsService, StatsIntervalData
from bxgateway import gateway_constants
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType


class EthGatewayStatInterval(StatsIntervalData):
    __slots__ = [
        "total_encryption_time",
        "total_encrypted_msgs_count",
        "max_encryption_time",
        "total_decryption_time",
        "total_decrypted_msgs_count",
        "max_decryption_time",
        "total_serialization_time",
        "total_serialized_msgs_count",
        "max_serialization_time"
    ]

    def __init__(self, *args, **kwargs):
        super(EthGatewayStatInterval, self).__init__(*args, **kwargs)
        self.total_encryption_time: float = 0
        self.total_encrypted_msgs_count: int = 0
        self.max_encryption_time: float = 0
        self.total_decryption_time: float = 0
        self.total_decrypted_msgs_count: int = 0
        self.max_decryption_time: float = 0
        self.total_serialization_time: float = 0
        self.total_serialized_msgs_count: int = 0
        self.max_serialization_time: float = 0


class _EthGatewayStatsService(StatisticsService):
    INTERVAL_DATA_CLASS = EthGatewayStatInterval

    def __init__(self, interval: int = gateway_constants.ETH_GATEWAY_STATS_INTERVAL,
                 look_back: int = gateway_constants.ETH_GATEWAY_STATS_LOOKBACK):
        self.interval_data: EthGatewayStatInterval = None
        super(_EthGatewayStatsService, self).__init__("EthGatewayStatsService", interval, look_back, reset=True,
                                                      logger=logging.get_logger(LogRecordType.TransactionStats, __name__))

    def log_encrypted_message(self, time: float) -> None:
        self.interval_data.total_encryption_time += time
        self.interval_data.total_encrypted_msgs_count += 1
        self.interval_data.max_encryption_time = max(self.interval_data.max_encryption_time, time)

    def log_decrypted_message(self, time: float) -> None:
        self.interval_data.total_decryption_time += time
        self.interval_data.total_decrypted_msgs_count += 1
        self.interval_data.max_decryption_time = max(self.interval_data.max_decryption_time, time)

    def log_serialized_message(self, time: float) -> None:
        self.interval_data.total_serialization_time += time
        self.interval_data.total_serialized_msgs_count += 1
        self.interval_data.max_serialization_time = max(self.interval_data.max_serialization_time, time)

    def get_info(self):
        if self.interval_data.total_encryption_time > 0:
            average_encryption_time = self.interval_data.total_encryption_time / \
                                      self.interval_data.total_encrypted_msgs_count
        else:
            average_encryption_time = 0

        if self.interval_data.total_decryption_time > 0:
            average_decryption_time = self.interval_data.total_decryption_time / \
                                      self.interval_data.total_decrypted_msgs_count
        else:
            average_decryption_time = 0

        if self.interval_data.total_serialization_time > 0:
            average_serialization_time = self.interval_data.total_serialization_time / \
                                         self.interval_data.total_serialized_msgs_count
        else:
            average_serialization_time = 0

        return {
            "total_encrypted_msgs_count": self.interval_data.total_encrypted_msgs_count,
            "average_encryption_time": stats_format.duration(average_encryption_time * 1000),
            "max_encryption_time": stats_format.duration(self.interval_data.max_encryption_time * 1000),
            "total_decrypted_msgs_count": self.interval_data.total_decrypted_msgs_count,
            "average_decryption_time": stats_format.duration(average_decryption_time * 1000),
            "max_decryption_time": stats_format.duration(self.interval_data.max_decryption_time * 1000),
            "total_serialized_msgs_count": self.interval_data.total_serialized_msgs_count,
            "average_serialization_time": stats_format.duration(average_serialization_time * 1000),
            "max_serialization_time": stats_format.duration(self.interval_data.max_serialization_time * 1000),
        }


eth_gateway_stats_service = _EthGatewayStatsService()
