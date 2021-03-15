import base64
from typing import TYPE_CHECKING, Optional

from bxcommon.rpc import rpc_constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.rpc_errors import RpcAccountIdError
from bxcommon.services import sdn_http_service

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class QuotaUsageRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {
        "params": "",
        "description": "return percentage used of daily transaction quota",
    }

    encoded_auth: str = ""

    def validate_params(self) -> None:
        params = self.params
        assert isinstance(params, dict)
        self.encoded_auth = base64.b64encode(self.headers.encode("utf-8")).decode("utf-8")

    async def process_request(self) -> JsonRpcResponse:
        if self.node.account_id is not None:
            account_id = self.get_account_id()
            assert account_id is not None
            result = sdn_http_service.fetch_quota_status(account_id)
            if result:
                return self.ok(result)
            else:
                raise RpcAccountIdError(
                    self.request_id,
                    "Gateway is not registered with a valid account."
                )
        else:
            raise RpcAccountIdError(
                self.request_id,
                "Gateway is not registered with an account."
            )

    def get_account_id(self) -> Optional[str]:
        account_id = self.node.account_id

        if self.encoded_auth:
            account_model = sdn_http_service.fetch_account_model(self.encoded_auth)
            if account_model is not None:
                account_id = account_model.account_id

        return account_id
