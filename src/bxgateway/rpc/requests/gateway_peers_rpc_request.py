from aiohttp.web_response import Response
from aiohttp.web_exceptions import HTTPOk

from bxcommon.connections.connection_type import ConnectionType

from bxgateway.rpc.requests.abstract_gateway_rpc_request import AbstractGatewayRpcRequest


class GatewayPeersRpcRequest(AbstractGatewayRpcRequest):

    help = {
        "params": "",
        "description": "return gateway connected peers"
    }

    async def process_request(self) -> Response:
        data = []
        connection_pool = self._node.connection_pool
        connections = \
            list(connection_pool.get_by_connection_type(ConnectionType.BLOCKCHAIN_NODE)) + \
            list(connection_pool.get_by_connection_type(ConnectionType.RELAY_ALL)) + \
            list(connection_pool.get_by_connection_type(ConnectionType.REMOTE_BLOCKCHAIN_NODE)) + \
            list(connection_pool.get_by_connection_type(ConnectionType.GATEWAY))
        for conn in connections:
            connection_type = \
                ConnectionType.GATEWAY if conn.CONNECTION_TYPE in ConnectionType.GATEWAY else conn.CONNECTION_TYPE
            data.append(
                {
                    "id": conn.peer_id,
                    "type": str(connection_type),
                    "addr": repr(conn.endpoint),
                    "direction": str(conn.direction),
                    "state": str(conn.state),
                 }
            )

        return self._format_response(data, HTTPOk)
