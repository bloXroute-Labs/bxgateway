import asyncio
from asyncio import Future
from typing import Optional

from websockets import WebSocketServerProtocol

from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.rpc.json_rpc_response import JsonRpcResponse


class WsConnection:
    def __init__(
        self,
        websocket: WebSocketServerProtocol,
        path: str,
        rpc_handler: SubscriptionRpcHandler,
    ) -> None:
        self.ws = websocket
        self.path = path  # currently unused
        self.rpc_handler = rpc_handler

        self.request_handler: Optional[Future] = None
        self.publish_handler: Optional[Future] = None
        self.alive_handler: Optional[Future] = None

    async def handle(self) -> None:
        request_handler = asyncio.ensure_future(self.handle_request(self.ws, self.path))
        publish_handler = asyncio.ensure_future(self.handle_publications(self.ws, self.path))
        alive_handler = asyncio.ensure_future(self.rpc_handler.wait_for_close())

        self.request_handler = request_handler
        self.publish_handler = publish_handler
        self.alive_handler = alive_handler

        done, pending = await asyncio.wait(
            [request_handler, publish_handler, alive_handler], return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()

        await self.close()

    async def handle_request(self, websocket: WebSocketServerProtocol, _path: str) -> None:
        async for message in websocket:
            try:
                response = await self.rpc_handler.handle_request(message)
            except RpcError as err:
                response = JsonRpcResponse(err.id, error=err).to_jsons()
            await websocket.send(response)

    async def handle_publications(self, websocket: WebSocketServerProtocol, _path: str) -> None:
        while True:
            message = await self.rpc_handler.get_next_subscribed_message()
            await websocket.send(
                self.rpc_handler.serialize_cached_subscription_message(message)
            )

    async def close(self) -> None:
        self.rpc_handler.close()

        request_handler = self.request_handler
        if request_handler is not None:
            request_handler.cancel()

        publish_handler = self.publish_handler
        if publish_handler is not None:
            publish_handler.cancel()

        alive_handler = self.alive_handler
        if alive_handler is not None:
            alive_handler.cancel()

        # cleanup to avoid circular reference and allow immediate GC.
        self.request_handler = None
        self.publish_handler = None
        self.alive_handler = None

        await self.ws.close()

