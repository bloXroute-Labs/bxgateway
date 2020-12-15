from typing import Optional, List, Union, Any, Dict, Tuple

import websockets
import ssl

from bxcommon import constants
from bxcommon.rpc.provider.abstract_ws_provider import AbstractWsProvider
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.rpc.rpc_request_type import RpcRequestType


class WsProvider(AbstractWsProvider):
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

    def __init__(
        self,
        uri: str,
        retry_connection: bool = False,
        queue_limit: int = constants.WS_PROVIDER_MAX_QUEUE_SIZE,
        headers: Optional[Dict] = None,
        skip_ssl_cert_verify: bool = False
    ):
        super(WsProvider, self).__init__(uri, retry_connection, queue_limit, headers)
        if uri.startswith("wss"):
            ssl_context = ssl.SSLContext()
            assert ssl_context is not None
            if skip_ssl_cert_verify:
                ssl_context.check_hostname = False
            self.ssl_context = ssl_context
        else:
            self.ssl_context = None

    async def connect_websocket(self) -> websockets.WebSocketClientProtocol:
        return await websockets.connect(self.uri, extra_headers=self.headers, ssl=self.ssl_context)

    async def call_bx(
        self,
        method: RpcRequestType,
        params: Union[List[Any], Dict[Any, Any], None],
        request_id: Optional[str] = None,
    ) -> JsonRpcResponse:
        if request_id is None:
            request_id = str(self.current_request_id)
            self.current_request_id += 1

        return await self.call(
            BxJsonRpcRequest(request_id, method, params)
        )

    async def subscribe(self, channel: str, options: Optional[Dict[str, Any]] = None) -> str:
        if options is None:
            options = {}
        response = await self.call_bx(
            RpcRequestType.SUBSCRIBE, [channel, options]
        )
        subscription_id = response.result
        assert isinstance(subscription_id, str)
        self.subscription_manager.register_subscription(subscription_id)
        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        response = await self.call_bx(RpcRequestType.UNSUBSCRIBE, [subscription_id])
        if response.result is not None:
            self.subscription_manager.unregister_subscription(subscription_id)
            return True, None
        else:
            return False, response.error
