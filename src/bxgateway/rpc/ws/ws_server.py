import asyncio
from typing import Optional, List, TYPE_CHECKING

import websockets
from websockets import WebSocketServerProtocol
from websockets.server import WebSocketServer

from bxcommon.feed.feed_manager import FeedManager
from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxgateway.rpc.ws.ws_connection import WsConnection
from bxutils import logging
from bxutils.encoding.json_encoder import Case

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class WsServer:
    def __init__(
        self,
        host: str,
        port: int,
        feed_manager: FeedManager,
        node: "AbstractGatewayNode",
        case: Case = Case.CAMEL,
    ) -> None:
        self.host = host
        self.port = port
        self.feed_manager = feed_manager
        self.node = node
        self.case = case
        self._started: bool = False

        self._server: Optional[WebSocketServer] = None
        self._connections: List[WsConnection] = []

    def status(self) -> bool:
        return self._started

    async def start(self) -> None:
        logger.info("Started websockets server")
        self._server = await websockets.serve(self.handle_connection, self.host, self.port)
        self._started = True

    async def stop(self) -> None:
        server = self._server
        self._started = False
        if server is not None:
            await asyncio.gather(
                *(connection.close() for connection in self._connections)
            )

            server.close()
            await server.wait_closed()

    async def handle_connection(self, websocket: WebSocketServerProtocol, path: str) -> None:
        logger.trace("Accepting new websocket connection...")
        connection = WsConnection(
            websocket,
            path,
            SubscriptionRpcHandler(self.node, self.feed_manager, self.case)
        )
        self._connections.append(connection)
        await connection.handle()
        self._connections.remove(connection)
