from typing import TYPE_CHECKING, Dict, Union, Optional, Any

from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcAccountIdError
from bxutils import logging

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)

TAG_TYPE = Union[str, int]


class GatewayBlxrCallRpcRequest(AbstractRpcRequest["EthGatewayNode"]):
    help = {
        "params": f"[{rpc_constants.TRANSACTION_JSON_PARAMS_KEY}: [transaction payload as json]]. "
                  f"[{rpc_constants.TAG_PARAMS_KEY}: [block number, or the string latest, earliest or pending]]",
        "description": "execute eth call through the BDN"
    }

    def validate_params(self) -> None:
        params = self.params
        if params is None or not isinstance(params, dict):
            raise RpcInvalidParams(
                self.request_id,
                "Params request field is either missing or not a dictionary type."
            )
        transaction_json = params.get(rpc_constants.TRANSACTION_JSON_PARAMS_KEY)
        if not transaction_json or not isinstance(transaction_json, dict):
            raise RpcInvalidParams(
                self.request_id,
                f"Invalid transaction request key: {rpc_constants.TRANSACTION_JSON_PARAMS_KEY} is required"
            )
        if rpc_constants.TAG_PARAMS_KEY in params:
            tag = params[rpc_constants.TAG_PARAMS_KEY]
            if isinstance(tag, int):
                pass
            elif isinstance(tag, str) and tag in {"latest", "pending", "earliest"}:
                pass
            else:
                raise RpcInvalidParams(
                    self.request_id,
                    f"Invalid value for {rpc_constants.TAG_PARAMS_KEY}: {tag}"
                )

    async def process_request(self) -> JsonRpcResponse:
        params = self.params
        assert isinstance(params, dict)
        account_id = self.get_account_id()
        if self.node.opts.blockchain_protocol != BlockchainProtocol.ETHEREUM:
            raise RpcInvalidParams(
                self.request_id,
                f"Gateway does not support {BlockchainProtocol.ETHEREUM} protocol methods"
            )
        if not account_id:
            raise RpcAccountIdError(
                self.request_id,
                "Gateway does not have an associated account. Please register the gateway with an account to submit "
                "calls through RPC."
            )

        # Hook, to verify EthNode WS connection health
        self.node.on_new_subscriber_request()

        transaction_obj: Dict[str, Any] = params[rpc_constants.TRANSACTION_JSON_PARAMS_KEY]
        tag: TAG_TYPE = params.get(rpc_constants.TAG_PARAMS_KEY, "latest")
        return await self.process_eth_call(transaction_obj, tag)

    async def process_eth_call(self, transaction_obj: Dict, tag: TAG_TYPE) -> JsonRpcResponse:
        response = await self.node.eth_ws_proxy_publisher.call_rpc(
            "eth_call",
            [transaction_obj, tag]
        )
        return self.ok({"result": response.result})

    def get_account_id(self) -> Optional[str]:
        return self.node.account_id
