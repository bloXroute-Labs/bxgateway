import asyncio
from abc import abstractmethod, ABCMeta
from asyncio import Future
from typing import Optional, Union, List, Any, Dict, Tuple, Coroutine, Callable, TypeVar

import websockets

from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_errors import RpcError
from bxgateway import log_messages
from bxgateway.rpc.provider.abstract_provider import AbstractProvider, SubscriptionNotification
from bxgateway.rpc.provider.response_queue import ResponseQueue
from bxgateway.rpc.provider.subscription_manager import SubscriptionManager
from bxutils import logging

T = TypeVar("T")
logger = logging.get_logger(__name__)


class WsException(Exception):
    def __init__(self, message: str):
        self.message = message


class AbstractWsProvider(AbstractProvider, metaclass=ABCMeta):
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

    @abstractmethod
    async def subscribe(self, channel: str, fields: Optional[List[str]] = None) -> str:
        pass

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        pass

    async def call_rpc(
        self,
        method: str,
        params: Union[List[Any], Dict[Any, Any], None],
        request_id: Optional[str] = None
    ) -> JsonRpcResponse:
        if request_id is None:
            request_id = str(self.current_request_id)
            self.current_request_id += 1

        return await self.call(
            JsonRpcRequest(request_id, method, params)
        )

    async def call(
        self,
        request: JsonRpcRequest
    ) -> JsonRpcResponse:
        request_id = request.id
        assert request_id is not None
        ws = self.ws
        assert ws is not None

        await ws.send(request.to_jsons())
        return await self.get_rpc_response(request_id)

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
            # noinspection PyBroadException
            try:
                response_message = JsonRpcResponse.from_jsons(next_message)
            except Exception:
                pass
            else:
                await self.response_messages.put(response_message)
                continue

            # process notification messages
            try:
                subscription_message = JsonRpcRequest.from_jsons(next_message)
                params = subscription_message.params
                assert isinstance(params, dict)
                await self.subscription_manager.receive_message(
                    SubscriptionNotification(
                        params["subscription"], params["result"],
                    )
                )
            except Exception as e:
                logger.warning(
                    log_messages.ETH_RPC_PROCESSING_ERROR,
                    next_message,
                    e,
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
