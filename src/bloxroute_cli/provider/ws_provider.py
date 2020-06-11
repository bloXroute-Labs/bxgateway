import asyncio
from asyncio import Future
from typing import Optional, List, Callable, Union, Any, Dict, Tuple, cast, Coroutine, TypeVar

import websockets

from bloxroute_cli.provider.abstract_provider import SubscriptionNotification, AbstractProvider
from bloxroute_cli.provider.response_queue import ResponseQueue
from bloxroute_cli.provider.subscription_manager import SubscriptionManager
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.rpc.rpc_request_type import RpcRequestType

T = TypeVar("T")


class WsException(Exception):
    def __init__(self, message: str):
        self.message = message


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
        self.ws_checker: Optional[Future] = None

        self.subscription_manager = SubscriptionManager()
        self.response_messages = ResponseQueue()

        self.running = False
        self.current_request_id = 1

    async def initialize(self) -> None:
        self.ws = await websockets.connect(self.uri)
        self.listener = asyncio.create_task(self.receive())
        self.ws_checker = asyncio.create_task(self.check_ws_close_status())
        self.running = True

    async def call(
        self,
        method: RpcRequestType,
        params: Union[List[Any], Dict[Any, Any], None],
        request_id: Optional[str] = None
    ) -> JsonRpcResponse:
        if request_id is None:
            request_id = str(self.current_request_id)
            self.current_request_id += 1
        ws = self.ws
        assert ws is not None
        await ws.send(BxJsonRpcRequest(request_id, method, params).to_jsons())
        return await self.get_rpc_response(request_id)

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

        async for next_message in ws:
            # process response messages
            try:
                response_message = JsonRpcResponse.from_jsons(next_message)
            except Exception:
                pass
            else:
                await self.response_messages.put(response_message)
                continue

            # process notification messages
            subscription_message = BxJsonRpcRequest.from_jsons(next_message)
            params = subscription_message.params
            assert isinstance(params, dict)
            await self.subscription_manager.receive_message(
                SubscriptionNotification(
                    params["subscription"], params["result"],
                )
            )
        self.running = False

    async def get_next_subscription_notification(self) -> SubscriptionNotification:
        return await self._wait_for_ws_message(
            self.subscription_manager.get_next_subscription_notification()
        )

    async def get_next_subscription_notification_by_id(
        self, subscription_id: str
    ) -> SubscriptionNotification:
        return await self._wait_for_ws_message(
            self.subscription_manager.get_next_subscription_notification_for_id(
                subscription_id
            )
        )

    async def get_rpc_response(self, request_id: str) -> JsonRpcResponse:
        return await self._wait_for_ws_message(
            self.response_messages.get_by_request_id(request_id)
        )

    async def _wait_for_ws_message(self, ws_waiter: Coroutine[Any, Any, T]) -> T:
        ws_waiter_task = asyncio.create_task(ws_waiter)
        ws_checker_task = self.ws_checker
        assert ws_checker_task is not None

        await asyncio.wait(
            [ws_waiter_task, ws_checker_task], return_when=asyncio.FIRST_COMPLETED
        )

        if ws_waiter_task.done():
            return ws_waiter_task.result()
        else:
            ws_waiter_task.cancel()

        raise WsException(
            "Websocket connection seems to be broken. Try reconnecting or checking "
            "that you are using the correct certificates."
        )

    async def check_ws_close_status(self) -> None:
        ws = self.ws
        assert ws is not None
        await ws.wait_closed()
        self.running = False
        await self.close()

    async def close(self) -> None:
        ws = self.ws
        if ws is not None:
            await ws.close()
            await ws.wait_closed()

        listener = self.listener
        if listener is not None:
            listener.cancel()

        ws_checker = self.ws_checker
        if ws_checker is not None:
            ws_checker.cancel()

        self.running = False
