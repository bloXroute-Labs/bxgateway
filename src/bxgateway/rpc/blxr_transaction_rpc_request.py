from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPBadRequest, HTTPAccepted
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.exceptions import ParseError
from bxcommon.models.tx_quota_type_model import TxQuotaType

from bxcommon.utils import convert
from bxcommon.utils.stats.transaction_stat_event_type import TransactionStatEventType
from bxcommon.utils.stats.transaction_statistics_service import tx_stats
from bxgateway.rpc.abstract_rpc_request import AbstractRpcRequest


class BlxrTransactionRpcRequest(AbstractRpcRequest):
    TRANSACTION: str = "transaction"
    QUOTA_TYPE: str = "quota_type"
    help = {
        "params": f"[Required - {TRANSACTION}: [transaction payload in hex string format],"
        f"Optional - {QUOTA_TYPE}: [{TxQuotaType.PAID_DAILY_QUOTA.name.lower()} for binding with a paid account"
        f"(default) or {TxQuotaType.FREE_DAILY_QUOTA.name.lower()}]]"
    }

    async def process_request(self) -> Response:
        try:
            assert self.params is not None and isinstance(self.params, dict)
            transaction_str = self.params[self.TRANSACTION]
            transaction = convert.hex_to_bytes(transaction_str)
        except TypeError:
            raise HTTPBadRequest(text=f"Invalid transaction request params type: {self.params}!")
        except KeyError:
            raise HTTPBadRequest(text=f"Missing {self.TRANSACTION} field in params object: {self.params}!")
        except AssertionError:
            raise HTTPBadRequest(text="Params request field is either missing or not a dictionary type!")
        network_num = self._node.network_num
        account_id = self._node.account_id
        quota_type_str = self.params.get(self.QUOTA_TYPE, TxQuotaType.PAID_DAILY_QUOTA.name).upper()
        if quota_type_str in TxQuotaType:
            quota_type = TxQuotaType[quota_type_str]
        elif account_id is None:
            quota_type = TxQuotaType.FREE_DAILY_QUOTA
        else:
            quota_type = TxQuotaType.PAID_DAILY_QUOTA
        try:
            message_converter = self._node.message_converter
            assert message_converter is not None, "Invalid server state!"
            bx_tx = message_converter.bdn_tx_to_bx_tx(transaction, network_num, quota_type)
        except (ValueError, ParseError):
            raise HTTPBadRequest(text=f"Invalid transaction param: {transaction_str} was provided!")
        tx_service = self._node.get_tx_service()
        tx_hash = bx_tx.tx_hash()
        if tx_service.has_transaction_contents(tx_hash):
            short_id = tx_service.get_short_id(tx_hash)
            tx_stats.add_tx_by_hash_event(
                tx_hash,
                TransactionStatEventType.BDN_TX_RECEIVED_FROM_CLIENT_ACCOUNT_IGNORE_SEEN,
                network_num,
                account_id=account_id,
                short_id=short_id
            )
            raise HTTPBadRequest(text=f"Transaction {bx_tx.tx_hash} was already seen!")
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.BDN_TX_RECEIVED_FROM_CLIENT_ACCOUNT,
            network_num,
            account_id=account_id
        )

        # All connections outside of this one is a bloXroute server
        broadcast_peers = self._node.broadcast(bx_tx, connection_types=[ConnectionType.RELAY_TRANSACTION])
        tx_stats.add_tx_by_hash_event(
            tx_hash,
            TransactionStatEventType.TX_SENT_FROM_GATEWAY_TO_PEERS,
            network_num,
            peers=map(lambda conn: (conn.peer_desc, conn.CONNECTION_TYPE), broadcast_peers)
        )
        tx_service.set_transaction_contents(tx_hash, bx_tx.tx_val())
        return self._format_response(f"Transaction {tx_hash} was successfully submitted to the BDN.", HTTPAccepted)
