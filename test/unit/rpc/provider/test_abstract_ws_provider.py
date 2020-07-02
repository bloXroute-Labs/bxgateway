import asyncio
from typing import Tuple, Optional, List, Any, Union, Iterable, AsyncIterable
from unittest.mock import patch

import websockets

from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway import gateway_constants
from bxgateway.rpc.provider.abstract_ws_provider import AbstractWsProvider, WsException


class TestWebSocket(websockets.WebSocketClientProtocol):
    def __init__(self):
        self.alive_event = asyncio.Event()
        self.send_messages = []
        self.recv_messages: 'asyncio.Queue[websockets.Data]' = asyncio.Queue()

    async def wait_closed(self) -> None:
        await self.alive_event.wait()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.alive_event.set()

    async def send(
        self,
        message: Union[websockets.Data, Iterable[websockets.Data], AsyncIterable[websockets.Data]],
    ) -> None:
        self.send_messages.append(message)

    async def recv(self) -> websockets.Data:
        return await self.recv_messages.get()


class TestWsProvider(AbstractWsProvider):
    def __init__(self, uri: str):
        super().__init__(uri)
        self.fail_connect = False

    async def subscribe(self, channel: str, fields: Optional[List[str]] = None) -> str:
        pass

    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        pass

    async def connect_websocket(self) -> websockets.WebSocketClientProtocol:
        if self.fail_connect:
            raise ConnectionRefusedError
        else:
            return TestWebSocket()

    # just for autocompletion
    def get_test_websocket(self) -> Optional[TestWebSocket]:
        return self.ws


class AbstractWsProviderTest(AbstractTestCase):

    @async_test
    async def setUp(self) -> None:
        self.provider = TestWsProvider("ws://")

    @async_test
    async def test_startup(self):
        await self.provider.initialize()

        self.assertEqual(True, self.provider.running)
        self.assertIsNotNone(self.provider.ws)
        self.assertIsNotNone(self.provider.listener_task)
        self.assertIsNotNone(self.provider.ws_status_check)
        self.assertTrue(self.provider.connected_event.is_set())

    @async_test
    async def test_send_receive_messages(self):
        await self.provider.initialize()

        rpc_message = JsonRpcResponse("1", "foo")
        await self.provider.get_test_websocket().recv_messages.put(
            rpc_message.to_jsons()
        )

        emitted_message = await self.provider.get_rpc_response("1")
        self.assertEqual(rpc_message, emitted_message)

    @async_test
    async def test_disconnect_on_start(self):
        self.provider.fail_connect = True

        with self.assertRaises(ConnectionRefusedError):
            await self.provider.initialize()

    @async_test
    async def test_disconnect_after_startup_no_retry(self):
        await self.provider.initialize()

        await self.provider.ws.close()
        await asyncio.sleep(0)

        self.assertFalse(self.provider.running)
        self.assertTrue(self.provider.listener_task.cancelled())
        self.assertTrue(self.provider.ws_status_check.done())

    @async_test
    async def test_disconnect_after_startup_retry(self):
        gateway_constants.WS_MIN_RECONNECT_TIMEOUT_S = 10

        self.provider.retry_connection = True
        await self.provider.initialize()

        await self.provider.ws.close()
        await asyncio.sleep(0)

        self.assertTrue(self.provider.running)
        self.assertFalse(self.provider.connected_event.is_set())

        rpc_message = JsonRpcResponse("1", "foo")
        await self.provider.get_test_websocket().recv_messages.put(
            rpc_message.to_jsons()
        )
        with self.assertRaises(WsException):
            await self.provider.get_rpc_response("1")

        send_task = asyncio.create_task(self.provider.call(JsonRpcRequest("2", "123", None)))
        await asyncio.sleep(0.1)
        self.assertFalse(send_task.done())
        self.assertEqual(0, len(self.provider.get_test_websocket().send_messages))

        # reconnect now
        await self.provider.connect()
        await asyncio.sleep(0)

        # re-emit new message
        await self.provider.get_test_websocket().recv_messages.put(
            rpc_message.to_jsons()
        )
        received_message = await self.provider.get_rpc_response("1")
        self.assertEqual(rpc_message, received_message)

        # still waiting for response
        self.assertFalse(send_task.done())
        self.assertEqual(1, len(self.provider.get_test_websocket().send_messages))

        # task finished
        response_rpc_message = JsonRpcResponse("2", "foo")
        await self.provider.get_test_websocket().recv_messages.put(
            response_rpc_message.to_jsons()
        )
        await asyncio.sleep(0.01)
        self.assertTrue(send_task.done())
        self.assertEqual(response_rpc_message, send_task.result())

    @async_test
    async def tearDown(self) -> None:
        await self.provider.close()
        await asyncio.sleep(0.01)
