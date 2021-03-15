from typing import TYPE_CHECKING
from datetime import datetime

from bxcommon.rpc import rpc_constants
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.utils import config
from bxutils import logging

logger = logging.get_logger(__name__)


if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

DEFAULT_TX_SERVICE_FILE_NAME = "tx_service_dump.csv"


class GatewayTransactionServiceRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):

    help = {
        "params": f"Optional - {rpc_constants.TX_SERVICE_FILE_NAME_PARAMS_KEY}: ",
        "description": "dump transaction service to file"
    }

    def __init__(
        self,
        request: BxJsonRpcRequest,
        node: "AbstractGatewayNode",
    ):
        self._file_name = DEFAULT_TX_SERVICE_FILE_NAME
        super().__init__(request, node)

    def validate_params(self) -> None:
        super().validate_params()
        params = self.params
        assert isinstance(params, dict)
        if rpc_constants.TX_SERVICE_FILE_NAME_PARAMS_KEY in params:
            self._file_name = params[rpc_constants.TX_SERVICE_FILE_NAME_PARAMS_KEY]

    async def process_request(self) -> JsonRpcResponse:
        path = config.get_data_file(self._file_name)
        with open(path, "w", encoding="utf-8") as f:
            f.write("transaction_hash,has_contents,has_short_id,contents_length,short_id,time_inserted\n")

            tx_service = self.node.get_tx_service()
            for tx_hash in tx_service.iter_transaction_hashes():
                transaction_cache_key = tx_service._tx_hash_to_cache_key(tx_hash)
                has_contents = transaction_cache_key in tx_service._tx_cache_key_to_contents
                has_short_id = transaction_cache_key in tx_service._tx_cache_key_to_short_ids

                contents_length = 0
                if has_contents:
                    tx_contents = tx_service._tx_cache_key_to_contents[transaction_cache_key]
                    contents_length = len(tx_contents)

                if has_short_id:
                    short_ids = tx_service._tx_cache_key_to_short_ids[transaction_cache_key]
                    for short_id in short_ids:
                        time_inserted = tx_service.get_short_id_assign_time(short_id)
                        if time_inserted == 0.0:
                            time_inserted = None
                        else:
                            time_inserted = datetime.fromtimestamp(time_inserted)

                        f.write(
                            f"{tx_hash},"
                            f"{has_contents},"
                            f"{has_short_id},"
                            f"{contents_length},"
                            f"{short_id},"
                            f"{time_inserted}\n"
                        )
                else:
                    f.write(
                        f"{tx_hash},"
                        f"{has_contents},"
                        f"{has_short_id},"
                        f"{contents_length},"
                        f"{None},"
                        f"{None}\n"
                    )
            f.close()

        return self.ok({
            "file": path
        })
