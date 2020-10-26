from typing import Any, Callable, Optional, Dict
from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

logger_filters = logging.get_logger(LogRecordType.TransactionFiltering, __name__)

MAX_RECURSION_DEPTH = 10

# DSL = Domain Specific Language

operations = {
    "AND": all,
    "OR": any,
}


def handle(
    filters: Optional[Dict[str, Any]],
    handle_filter: Callable[[str, Any, Any], bool],
    transaction: Any,
    recursion_depth: int = 1,
):
    """Executes the json-logic with given data."""
    # You've recursed to a primitive, stop!
    if recursion_depth == MAX_RECURSION_DEPTH:
        return False
    if filters is None or not isinstance(filters, dict):
        return filters
    logger_filters.trace(f"handling filters :{filters}")

    operator = list(filters.keys())[0]
    values = filters[operator]
    # Easy syntax for unary operators, like {"var": "x"} instead of strict
    # {"var": ["x"]}
    if not isinstance(values, list):
        values = [values]

    if operator not in operations:
        return handle_filter(operator, values, transaction)
    # Recursion!
    values = [
        handle(val, handle_filter, transaction, recursion_depth=recursion_depth + 1)
        for val in values
    ]

    return operations[operator](values)


def reformat(
    filters: Any, reformat_filter: Callable, recursion_depth: int = 1
) -> Dict[str, Any]:
    """Executes the json-logic with given data."""
    if recursion_depth == MAX_RECURSION_DEPTH:
        raise RecursionError
    # You've recursed to a primitive, stop!
    if filters is None or not isinstance(filters, dict):
        return filters
    logger_filters.debug(f"Validating and reformatting filters :{filters}")
    operator = list(filters.keys())[0]
    values = filters[operator]
    # Easy syntax for unary operators, like {"var": "x"} instead of strict
    # {"var": ["x"]}
    if not isinstance(values, list):
        values = [values]
    if operator not in operations:
        filters[operator] = reformat_filter(operator, values)
        return filters

    # Recursion!
    values = [
        reformat(val, reformat_filter, recursion_depth=recursion_depth + 1)
        for val in values
    ]

    return filters
