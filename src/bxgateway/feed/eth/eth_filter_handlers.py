from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

logger_filters = logging.get_logger(LogRecordType.TransactionFiltering, __name__)


def reformat_tx_value(value: str) -> float:
    return float(int(value, 0))
