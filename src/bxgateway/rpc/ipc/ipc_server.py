import asyncio
import os
from typing import Optional, List, TYPE_CHECKING
from bxcommon.feed.feed_manager import FeedManager
from bxgateway.rpc.ws.ws_connection import WsConnection
from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxutils import logging
from bxcommon.utils import config

import websockets
from websockets import WebSocketServerProtocol
from websockets.server import WebSocketServer

from bxutils.encoding.json_encoder import Case

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class IpcServer:
    def __init__(
        self,
        ipc_file: str,
        feed_manager: FeedManager,
        node: "AbstractGatewayNode",
        case: Case = Case.CAMEL
    ):
        self.ipc_path = config.get_data_file(ipc_file)
        self.node = node
        self.feed_manager = feed_manager
        self.case = case
        self._server: Optional[WebSocketServer] = None
        self._connections: List[WsConnection] = []
        self._started: bool = False

    def status(self) -> bool:
        return self._started

    async def start(self) -> None:
        if os.path.exists(self.ipc_path):
            os.remove(self.ipc_path)
        self._server = await websockets.unix_serve(self.handle_connection, self.ipc_path)
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
        if os.path.exists(self.ipc_path):
            os.remove(self.ipc_path)

    async def handle_connection(self, websocket: WebSocketServerProtocol, path: str) -> None:
        logger.trace("Accepting new IPC connection...")
        connection = WsConnection(
            websocket,
            path,
            SubscriptionRpcHandler(self.node, self.feed_manager, self.case)
        )
        self._connections.append(connection)
        await connection.handle()
        self._connections.remove(connection)
