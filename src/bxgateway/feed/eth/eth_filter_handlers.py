from typing import Any, Union, List
from bxgateway.feed.eth.eth_transaction_feed_entry import EthTransactionFeedEntry
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

logger_filters = logging.get_logger(LogRecordType.TransactionFiltering, __name__)


def reformat_tx_value_range(value: Any) -> List[float]:
    assert isinstance(value, list) or isinstance(value, tuple)
    assert 0 <= len(value) < 3
    if len(value) == 0:
        return [0.0, float("inf")]
    if len(value) == 1:
        return [float(int(value[0], 0)), float("inf")]
    return [float(int(value[0], 0)), float(int(value[1], 0))]


def reformat_tx_address(value: Any) -> List[str]:
    assert isinstance(value, list)
    tx_from_list = []
    for address in value:
        assert isinstance(address, str)
        tx_from_list.append(address.lower())
    return tx_from_list


def handle_tx_value_range(
    value_range: List[float], tx: EthTransactionFeedEntry
) -> bool:
    value = tx.tx_contents.get("value", None)
    if not value:
        return False
    if value_range[0] <= float(int(value, 0)) <= value_range[1]:
        return True
    return False


def handle_from(value: Any, tx: EthTransactionFeedEntry) -> bool:
    from_address = tx.tx_contents.get("from", None)
    if not from_address:
        return False
    if value == from_address:
        return True
    return False


def handle_to(value: Any, tx: EthTransactionFeedEntry) -> bool:
    to_address = tx.tx_contents.get("to", None)
    logger_filters.debug(f"to address {to_address}, value {value}")
    if not to_address:
        return False
    if value == [to_address]:
        return True
    return False


FILTER_HANDLER_MAP = {
    "transaction_value_range_eth": handle_tx_value_range,
    "from": handle_from,
    "to": handle_to,
}

REFORMAT_FILTER_MAP = {
    "transaction_value_range_eth": reformat_tx_value_range,
    "from": reformat_tx_address,
    "to": reformat_tx_address,
}


def reformat_filter(filter_name: str, filter_contents: Any) -> Any:
    reformatter = REFORMAT_FILTER_MAP[filter_name]
    logger_filters.debug(
        "Reformatting with validator {} filters: {}",
        reformatter.__name__,
        filter_contents,
    )
    return reformatter(filter_contents)


def handle_filter(
    filter_name: str, filter_contents: Any, tx: EthTransactionFeedEntry
) -> bool:
    handler = FILTER_HANDLER_MAP[filter_name]
    logger_filters.debug(
        "handling with handler {} filters: {}", handler.__name__, filter_contents
    )
    return handler(filter_contents, tx)
