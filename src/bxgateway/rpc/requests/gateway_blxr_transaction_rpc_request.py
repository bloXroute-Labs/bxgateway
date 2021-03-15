from typing import TYPE_CHECKING
import asyncio

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.exceptions import ParseError
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_blxr_transaction_rpc_request import AbstractBlxrTransactionRpcRequest
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcAccountIdError, RpcBlocked
from bxcommon.utils import convert
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.messages.eth.serializers.transaction import Transaction

from bxutils import logging, log_messages as common_log_messages

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class GatewayBlxrTransactionRpcRequest(AbstractBlxrTransactionRpcRequest["AbstractGatewayNode"]):
    SYNCHRONOUS = rpc_constants.SYNCHRONOUS_PARAMS_KEY
    synchronous: bool = True

    def validate_params(self) -> None:
        params = self.params
        if params is None or not isinstance(params, dict):
            raise RpcInvalidParams(
                self.request_id,
                "Params request field is either missing or not a dictionary type."
            )
        if (
            rpc_constants.TRANSACTION_JSON_PARAMS_KEY in params
            and self.get_network_protocol() == BlockchainProtocol.ETHEREUM
        ):
            tx_json = params[rpc_constants.TRANSACTION_JSON_PARAMS_KEY]
            tx_bytes = Transaction.from_json_with_validation(tx_json).contents().tobytes()
            params[rpc_constants.TRANSACTION_PARAMS_KEY] = tx_bytes.hex()

        super(GatewayBlxrTransactionRpcRequest, self).validate_params()

        if self.SYNCHRONOUS in params:
            synchronous = params[rpc_constants.SYNCHRONOUS_PARAMS_KEY]
            self.synchronous = convert.str_to_bool(str(synchronous).lower(), default=True)
        else:
            self.synchronous = GatewayBlxrTransactionRpcRequest.synchronous

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
        if not self.track_flag:
            self.track_flag = TransactionFlag.PAID_TX
        return await self.process_transaction(network_num, account_id, transaction_str)

    async def process_transaction(
        self, network_num: int, account_id: str, transaction_str: str
    ) -> JsonRpcResponse:

        if self.synchronous:
            return await self.post_process_transaction(
                network_num, account_id, self.track_flag, transaction_str
            )
        else:
            asyncio.create_task(
                self.post_process_transaction(
                    network_num, account_id, self.track_flag, transaction_str
                )
            )
        return JsonRpcResponse(
            self.request_id,
            {
                "tx_hash": "not available with async",
            }
        )

    async def post_process_transaction(
        self, network_num: int, account_id: str, transaction_flag: TransactionFlag, transaction_str: str
    ) -> JsonRpcResponse:
        try:
            message_converter = self.node.message_converter
            assert message_converter is not None, "Invalid server state!"
            transaction = message_converter.encode_raw_msg(transaction_str)
            bx_tx = message_converter.bdn_tx_to_bx_tx(transaction, network_num, transaction_flag, account_id)
        except (ValueError, ParseError) as e:
            logger.error(common_log_messages.RPC_COULD_NOT_PARSE_TRANSACTION, e)
            raise RpcInvalidParams(
                self.request_id, f"Invalid transaction param: {transaction_str}"
            )
        tx_service = self.node.get_tx_service()
        tx_hash = bx_tx.tx_hash()
        transaction_key = tx_service.get_transaction_key(tx_hash)
        if (
            tx_service.has_transaction_contents_by_key(transaction_key) or
            tx_service.removed_transaction_by_key(transaction_key)
        ):
            short_id = tx_service.get_short_id_by_key(transaction_key)
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.TX_RECEIVED_FROM_RPC_REQUEST_IGNORE_SEEN,
                network_num,
                account_id=account_id, short_id=short_id
            )
            tx_json = {
                "tx_hash": str(tx_hash),
            }
            return self.ok(tx_json)
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_RECEIVED_FROM_RPC_REQUEST,
            network_num,
            account_id=account_id
        )
        if self.node.has_active_blockchain_peer():
            blockchain_tx_message = self.node.message_converter.bx_tx_to_tx(bx_tx)
            self.node.broadcast(
                blockchain_tx_message,
                connection_types=(ConnectionType.BLOCKCHAIN_NODE,)
            )

        # All connections outside of this one is a bloXroute server
        broadcast_peers = self.node.broadcast(
            bx_tx,
            connection_types=(ConnectionType.RELAY_TRANSACTION,)
        )
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
            network_num,
            peers=broadcast_peers
        )
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_GATEWAY_RPC_RESPONSE_SENT,
            network_num
        )
        tx_service.set_transaction_contents_by_key(transaction_key, bx_tx.tx_val())
        tx_json = {
            "tx_hash": str(tx_hash),
        }
        if not self.node.account_model.is_account_valid():
            raise RpcAccountIdError(
                self.request_id,
                "The account associated with this gateway has expired. "
                "Please visit https://portal.bloxroute.com to renew your subscription."
            )
        if self.node.quota_level == constants.FULL_QUOTA_PERCENTAGE:
            raise RpcBlocked(
                self.request_id,
                "The account associated with this gateway has exceeded its daily transaction quota."
            )
        else:
            return self.ok(tx_json)

    def get_network_num(self) -> int:
        return self.node.network_num

    def get_account_id(self) -> str:
        return self.node.account_id

    def get_network_protocol(self) -> BlockchainProtocol:
        blockchain_protocol = self.node.opts.blockchain_protocol
        assert blockchain_protocol is not None
        return BlockchainProtocol(blockchain_protocol)
