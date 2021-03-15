from typing import TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class GatewayPeersRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {
        "params": "",
        "description": "return gateway connected peers"
    }

    def validate_params(self) -> None:
        pass

    async def process_request(self) -> JsonRpcResponse:
        data = []
        connection_pool = self.node.connection_pool
        connections = (
            list(connection_pool.get_by_connection_types((ConnectionType.BLOCKCHAIN_NODE,)))
            + list(connection_pool.get_by_connection_types((ConnectionType.RELAY_ALL,)))
            + list(connection_pool.get_by_connection_types((ConnectionType.REMOTE_BLOCKCHAIN_NODE,)))
            + list(connection_pool.get_by_connection_types((ConnectionType.GATEWAY,)))
        )
        for conn in connections:
            connection_type = (
                ConnectionType.GATEWAY
                if conn.CONNECTION_TYPE in ConnectionType.GATEWAY
                else conn.CONNECTION_TYPE
            )
            data.append(
                {
                    "id": conn.peer_id,
                    "type": str(connection_type),
                    "addr": repr(conn.endpoint),
                    "direction": str(conn.direction),
                    "state": str(conn.state),
                }
            )

        return self.ok(data)
