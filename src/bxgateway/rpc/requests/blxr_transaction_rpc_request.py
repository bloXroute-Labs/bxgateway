from bxcommon.models.quota_type_model import QuotaType

from aiohttp.web_exceptions import HTTPBadRequest, HTTPAccepted
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.exceptions import ParseError
from bxcommon.rpc import rpc_constants
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractRpcRequest
from bxutils import logging

logger = logging.get_logger(__name__)


class BlxrTransactionRpcRequest(AbstractRpcRequest):
    TRANSACTION = rpc_constants.TRANSACTION_PARAMS_KEY
    QUOTA_TYPE: str = "quota_type"
    help = {
        "params": f"[Required - {TRANSACTION}: [transaction payload in hex string format],"
        f"Optional - {QUOTA_TYPE}: [{QuotaType.PAID_DAILY_QUOTA.name.lower()} for binding with a paid account"
        f"(default) or {QuotaType.FREE_DAILY_QUOTA.name.lower()}]]"
    }

    def _process_message(self, network_num, account_id, quota_type, transaction_str):
        try:
            message_converter = self._node.message_converter
            assert message_converter is not None, "Invalid server state!"
            transaction = message_converter.encode_raw_msg(transaction_str)
            bx_tx = message_converter.bdn_tx_to_bx_tx(transaction, network_num, quota_type)
        except (ValueError, ParseError) as e:
            logger.error("Error parsing the transaction:\n{}", e)
            raise HTTPBadRequest(text=f"Invalid transaction param: {transaction_str} was provided!")
        tx_service = self._node.get_tx_service()
        tx_hash = bx_tx.tx_hash()
        if tx_service.has_transaction_contents(tx_hash):
            short_id = tx_service.get_short_id(tx_hash)
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.BDN_TX_RECEIVED_FROM_RPC_REQUEST_IGNORE_SEEN,
                network_num,
                account_id=account_id, short_id=short_id
            )
            raise HTTPBadRequest(text=f"Transaction [{tx_hash} was already seen!")
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.BDN_TX_RECEIVED_FROM_RPC_REQUEST,
            network_num,
            account_id=account_id
        )
        # All connections outside of this one is a bloXroute server
        broadcast_peers = self._node.broadcast(bx_tx, connection_types=[ConnectionType.RELAY_TRANSACTION])
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
            network_num,
            peers=stats_format.connections(broadcast_peers)
        )
        tx_stats.add_tx_by_hash_event(tx_hash,
                                      TransactionStatEventType.TX_GATEWAY_RPC_RESPONSE_SENT,
                                      network_num)
        tx_service.set_transaction_contents(tx_hash, bx_tx.tx_val())
        tx_json = {
            "tx_hash": repr(tx_hash),
            "quota_type": quota_type.name.lower(),
            "account_id": account_id
        }
        return self._format_response(
            tx_json,
            HTTPAccepted
        )