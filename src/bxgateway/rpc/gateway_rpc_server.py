import asyncio
import json
from typing import TYPE_CHECKING, Callable, Awaitable
from aiohttp import web
from aiohttp.web import Application, Request, Response, AppRunner, TCPSite
from aiohttp.web_exceptions import HTTPClientError
from bxgateway.rpc.request_formatter import RequestFormatter
from bxgateway.rpc.response_formatter import ResponseFormatter

from bxutils import logging

from bxgateway.rpc.rpc_request_handler import RPCRequestHandler

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


@web.middleware
async def request_middleware(request: Request, handler: Callable[[Request], Awaitable[Response]]) -> Response:
    request_formatter = RequestFormatter(request)
    logger.trace("Handling RPC request: {}.", request_formatter)
    response = await handler(request)
    logger.trace(
        "Finished handling request: {}, returning response: {}.", request_formatter, ResponseFormatter(response)
    )
    return response


class GatewayRpcServer:
    RUN_SLEEP_INTERVAL_S: int = 5

    def __init__(self, node: "AbstractGatewayNode"):
        self._node = node
        self._app = Application(middlewares=[request_middleware])
        self._app.add_routes([web.post("/", self.handle_request), web.get("/", self.handle_get_request)])
        self._runner = AppRunner(self._app)
        self._site = None
        self._handler = RPCRequestHandler(self._node)
        self._stop_requested = False
        self._stop_waiter = asyncio.get_event_loop().create_future()
        self._started = False

    async def run(self) -> None:
        try:
            await self._start()
            while not self._stop_requested:
                await asyncio.sleep(self.RUN_SLEEP_INTERVAL_S)
        finally:
            self._stop_waiter.set_result(True)

    async def start(self) -> None:
        if self._started:
            return
        try:
            await self._start()
        finally:
            self._stop_waiter.set_result(True)

    async def stop(self) -> None:
        self._stop_requested = True
        await self._stop_waiter
        await self._runner.cleanup()

    async def handle_request(self, request: Request) -> Response:
        try:
            return await self._handler.handle_request(request)
        except HTTPClientError as e:
            err_msg = e.text
            code = e.status_code
            request_id = self._handler.request_id
            response_json = {
                "result": None,
                "error": err_msg,
                "code": code,
                "message": err_msg,
                "id": request_id
            }
            e.text = json.dumps(response_json)
            return e

    async def handle_get_request(self, request: Request) -> Response:
        return web.json_response({
            "required_request_type": "POST",
            "required_headers":[{RPCRequestHandler.CONTENT_TYPE: RPCRequestHandler.PLAIN}],
            "payload_structures": await self._handler.help()
        })

    async def _start(self) -> None:
        self._started = True
        await self._runner.setup()
        opts = self._node.opts
        self._site = TCPSite(self._runner, opts.rpc_host, opts.rpc_port)
        await self._site.start()

    def _get_error_response(self, err_msg: str, code: int) -> str:
        response_json = {
            "result": None,
            "error": err_msg,
            "code": code,
            "message": err_msg,
            "id": self._handler.request_id
        }
        return json.dumps(response_json)
