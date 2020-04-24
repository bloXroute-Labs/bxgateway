from bxcommon.models.quota_type_model import QuotaType

from bxcommon.rpc import rpc_constants
from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractRpcRequest


class BlxrTransactionRpcRequest(AbstractRpcRequest):
    TRANSACTION = rpc_constants.TRANSACTION_PARAMS_KEY
    QUOTA_TYPE: str = "quota_type"
    help = {
        "params": f"[Required - {TRANSACTION}: [transaction payload in hex string format],"
        f"Optional - {QUOTA_TYPE}: [{QuotaType.PAID_DAILY_QUOTA.name.lower()} for binding with a paid account"
        f"(default) or {QuotaType.FREE_DAILY_QUOTA.name.lower()}]]"
    }

