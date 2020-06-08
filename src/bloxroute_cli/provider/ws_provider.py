import asyncio
from asyncio import Future
from typing import Optional, List, Callable, Union, Any, Dict, Tuple, cast

import websockets

from bloxroute_cli.provider.abstract_provider import SubscriptionNotification, AbstractProvider
from bloxroute_cli.provider.subscription_manager import SubscriptionManager
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.rpc.rpc_request_type import RpcRequestType


class WsProvider(AbstractProvider):
    """
    Provider that connects to bxgateway's websocket RPC endpoint.

    Usage:

    (with context manager, recommended)
    ```
    ws_uri = "ws://127.0.0.1:28333"
    async with WsProvider(ws_uri) as ws:
        subscription_id = await ws.subscribe("newTxs")
        while True:
            next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
            print(next_notification)  # or process it generally
    ```

    (without context manager)
    ```
    ws_uri = "ws://127.0.0.1:28333"
    try:
        ws = await WsProvider(ws_uri)
        subscription_id = await ws.subscribe("newTxs")
        while True:
            next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
            print(next_notification)  # or process it generally
    except:
        await ws.close()
    ```

    (callback interface)
    ```
    ws_uri = "ws://127.0.0.1:28333"
    ws = WsProvider(ws_uri)
    await ws.initialize()

    def process(subscription_message):
        print(subscription_message)

    ws.subscribe_with_callback(process, "newTxs")

    while True:
        await asyncio.sleep(0)  # otherwise program would exit
    ```
    """

    def __init__(self, uri: str):
        self.uri: str = uri

        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.listener: Optional[Future] = None
        self.subscription_manager = SubscriptionManager()
        self.response_messages: "asyncio.Queue[JsonRpcResponse]" = asyncio.Queue()
        self.running = False

    async def initialize(self) -> None:
        self.ws = await websockets.connect(self.uri)
        self.listener = asyncio.create_task(self.receive())
        self.running = True

    async def call(
        self, method: RpcRequestType, params: Union[List[Any], Dict[Any, Any], None]
    ) -> JsonRpcResponse:
        ws = self.ws
        assert ws is not None
        await ws.send(BxJsonRpcRequest(None, method, params).to_jsons())
        return await self.get_next_rpc_response()

    async def subscribe(self, channel: str, fields: Optional[List[str]] = None) -> str:
        response = await self.call(RpcRequestType.SUBSCRIBE, [channel, {"include": fields}])
        subscription_id = response.result
        assert isinstance(subscription_id, str)
        self.subscription_manager.register_subscription(subscription_id)
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        response = await self.call(RpcRequestType.UNSUBSCRIBE, [subscription_id])
        if response.result is not None:
            self.subscription_manager.unregister_subscription(subscription_id)
            return True, None
        else:
            return False, response.error

    def subscribe_with_callback(
        self,
        callback: Callable[[SubscriptionNotification], None],
        channel: str,
        fields: Optional[List[str]] = None,
    ) -> None:
        asyncio.create_task(self._handle_subscribe_callback(callback, channel, fields))

    async def _handle_subscribe_callback(
        self,
        callback: Callable[[SubscriptionNotification], None],
        channel: str,
        fields: Optional[List[str]] = None,
    ) -> None:
        subscription_id = await self.subscribe(channel, fields)
        while self.running:
            notification = await self.subscription_manager.get_next_subscription_notification_for_id(
                subscription_id
            )
            callback(notification)

    async def receive(self) -> None:
        ws = self.ws
        assert ws is not None

        while self.running:
            next_message = await ws.recv()
            # noinspection PyBroadException
            try:
                response_message = JsonRpcResponse.from_jsons(next_message)
            except Exception:
                pass
            else:
                await self.response_messages.put(response_message)
                continue

            subscription_message = BxJsonRpcRequest.from_jsons(next_message)
            params = subscription_message.params
            assert isinstance(params, dict)
            await self.subscription_manager.receive_message(
                SubscriptionNotification(
                    params["subscription"], params["result"],
                )
            )

    async def get_next_subscription_notification(self) -> SubscriptionNotification:
        return await self.subscription_manager.get_next_subscription_notification()

    async def get_next_subscription_notification_by_id(
        self, subscription_id: str
    ) -> SubscriptionNotification:
        return await self.subscription_manager.get_next_subscription_notification_for_id(
            subscription_id
        )

    async def get_next_rpc_response(self) -> JsonRpcResponse:
        return await self.response_messages.get()

    async def close(self) -> None:
        ws = self.ws
        if ws is not None:
            ws.close()
            await ws.wait_closed()

        listener = self.listener
        if listener is not None:
            listener.cancel()

        self.running = False
