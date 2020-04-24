from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPAccepted

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats

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

    def _send_tx_to_peers(self,
                          tx_hash: Sha256Hash,
                          network_num: int,
                          bx_tx: TxMessage,
                          tx_service: TransactionService):
        # All connections outside of this one is a bloXroute server
        broadcast_peers = self._node.broadcast(bx_tx, connection_types=[ConnectionType.RELAY_TRANSACTION])
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
            network_num,
            peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), broadcast_peers)
        )
        tx_service.set_transaction_contents(tx_hash, bx_tx.tx_val())

    def _send_response_to_client(self, tx_hash: Sha256Hash, quota_type: QuotaType, account_id: str) -> Response:
        tx_json = {
            "tx_hash": repr(tx_hash),
            "quota_type": quota_type.name.lower(),
            "account_id": account_id
        }
        return self._format_response(
            tx_json,
            HTTPAccepted
        )
