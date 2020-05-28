from typing import Optional, List, TYPE_CHECKING

import websockets
from websockets import WebSocketServerProtocol
from websockets.server import WebSocketServer

from bxgateway.feed.feed_manager import FeedManager
from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxgateway.rpc.ws.ws_connection import WsConnection

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class WsServer:
    def __init__(
        self, host: str, port: int, feed_manager: FeedManager, node: "AbstractGatewayNode"
    ) -> None:
        self.host = host
        self.port = port
        self.feed_manager = feed_manager
        self.node = node

        self._server: Optional[WebSocketServer] = None
        self._connections: List[WsConnection] = []

    async def start(self) -> None:
        self._server = await websockets.serve(self.handle_connection, self.host, self.port)

    async def stop(self) -> None:
        server = self._server
        if server is not None:
            for connection in self._connections:
                connection.close()

            server.close()
            await server.wait_closed()

    async def handle_connection(self, websocket: WebSocketServerProtocol, path: str) -> None:
        connection = WsConnection(
            websocket,
            path,
            SubscriptionRpcHandler(self.node, self.feed_manager)
        )
        self._connections.append(connection)
        await connection.handle()
        self._connections.remove(connection)