import asyncio
from asyncio import Future
from typing import Optional

from websockets import WebSocketServerProtocol

from bxgateway.rpc.subscription_rpc_handler import SubscriptionRpcHandler


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

    async def handle(self) -> None:
        request_handler = asyncio.ensure_future(self.handle_request(self.ws, self.path))
        publish_handler = asyncio.ensure_future(self.handle_publications(self.ws, self.path))

        self.request_handler = request_handler
        self.publish_handler = publish_handler

        done, pending = await asyncio.wait(
            [request_handler, publish_handler], return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()

        self.close()

    async def handle_request(self, websocket: WebSocketServerProtocol, _path: str) -> None:
        async for message in websocket:
            response = await self.rpc_handler.handle_request(message)
            await websocket.send(response)

    async def handle_publications(self, websocket: WebSocketServerProtocol, _path: str) -> None:
        while True:
            message = await self.rpc_handler.get_next_subscribed_message()
            await websocket.send(message.to_jsons())

    def close(self) -> None:
        self.rpc_handler.close()

        request_handler = self.request_handler
        if request_handler is not None:
            request_handler.cancel()

        publish_handler = self.publish_handler
        if publish_handler is not None:
            publish_handler.cancel()
