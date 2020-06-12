from typing import TYPE_CHECKING

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

    def validate_params(self) -> None:
        pass

    async def process_request(self) -> JsonRpcResponse:
        if self.node.account_id is not None:
            account_id = self.node.account_id
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
