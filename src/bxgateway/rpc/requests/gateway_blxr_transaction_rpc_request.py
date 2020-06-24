from typing import TYPE_CHECKING
import asyncio

from bxcommon.models.quota_type_model import QuotaType
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.exceptions import ParseError
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_blxr_transaction_rpc_request import \
    AbstractBlxrTransactionRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcAccountIdError
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxutils import logging, log_messages as common_log_messages

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class GatewayBlxrTransactionRpcRequest(AbstractBlxrTransactionRpcRequest["AbstractGatewayNode"]):
    QUOTA_TYPE: str = "quota_type"
    SYNCHRONOUS = rpc_constants.SYNCHRONOUS_PARAMS_KEY

    def __init__(self, request: BxJsonRpcRequest, node: "AbstractGatewayNode") -> None:
        super().__init__(request, node)
        params = self.params
        assert isinstance(params, dict)

        lowercase_true = str(True).lower()
        self.synchronous = \
            params.get(self.SYNCHRONOUS, lowercase_true).lower() == lowercase_true

    async def process_request(self) -> JsonRpcResponse:
        params = self.params
        assert isinstance(params, dict)

        account_id = self.get_account_id()
        if not account_id:
            raise RpcAccountIdError(
                self.request_id,
                "Gateway does not have an associated account. Please register the gateway with an account to submit "
                "transactions through RPC."
            )

        transaction_str: str = params[rpc_constants.TRANSACTION_PARAMS_KEY]
        network_num = self.get_network_num()
        quota_type = QuotaType.PAID_DAILY_QUOTA
        return await self.process_transaction(network_num, account_id, quota_type, transaction_str)

    async def process_transaction(
        self, network_num: int, account_id: str, quota_type: QuotaType, transaction_str: str
    ) -> JsonRpcResponse:

        if not(self.node.account_model and self.node.account_model.cloud_api.is_service_valid()):
            raise RpcAccountIdError(
                self.request_id, "Account does not have cloud-api service. Please contact bloXroute support."
            )

        if self.synchronous:
            return await self.post_process_transaction(
                network_num, account_id, quota_type, transaction_str
            )
        else:
            asyncio.create_task(
                self.post_process_transaction(
                    network_num, account_id, quota_type, transaction_str
                )
            )
        return JsonRpcResponse(
            self.request_id,
            {
                "tx_hash": "not available with async",
                "quota_type": quota_type.name.lower(),
                "synchronous": str(self.synchronous)
            }
        )

    async def post_process_transaction(
        self, network_num: int, account_id: str, quota_type: QuotaType, transaction_str: str
    ) -> JsonRpcResponse:
        try:
            message_converter = self.node.message_converter
            assert message_converter is not None, "Invalid server state!"
            transaction = message_converter.encode_raw_msg(transaction_str)
            bx_tx = message_converter.bdn_tx_to_bx_tx(transaction, network_num, quota_type)
        except (ValueError, ParseError) as e:
            logger.error(common_log_messages.RPC_COULD_NOT_PARSE_TRANSACTION, e)
            raise RpcInvalidParams(
                self.request_id, f"Invalid transaction param: {transaction_str}"
            )
        tx_service = self.node.get_tx_service()
        tx_hash = bx_tx.tx_hash()
        if tx_service.has_transaction_contents(tx_hash) or tx_service.removed_transaction(tx_hash):
            short_id = tx_service.get_short_id(tx_hash)
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_RECEIVED_FROM_RPC_REQUEST_IGNORE_SEEN,
                network_num,
                account_id=account_id, short_id=short_id
            )
            tx_json = {
                "tx_hash": str(tx_hash),
                "quota_type": quota_type.name.lower(),
                "account_id": account_id,
            }
            return self.ok(tx_json)
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_RECEIVED_FROM_RPC_REQUEST,
            network_num,
            account_id=account_id
        )
        # All connections outside of this one is a bloXroute server
        broadcast_peers = self.node.broadcast(bx_tx, connection_types=[ConnectionType.RELAY_TRANSACTION])
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
            network_num,
            peers=stats_format.connections(broadcast_peers)
        )
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_GATEWAY_RPC_RESPONSE_SENT,
            network_num
        )
        tx_service.set_transaction_contents(tx_hash, bx_tx.tx_val())
        tx_json = {
            "tx_hash": str(tx_hash),
            "quota_type": quota_type.name.lower(),
            "account_id": account_id
        }
        return self.ok(tx_json)

    def get_network_num(self) -> int:
        return self.node.network_num

    def get_account_id(self) -> str:
        return self.node.account_id
