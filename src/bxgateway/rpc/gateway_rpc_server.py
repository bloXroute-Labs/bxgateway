from typing import TYPE_CHECKING

from aiohttp import web
from aiohttp.web import Request, Response
from aiohttp.web_exceptions import HTTPUnauthorized, HTTPClientError
from prometheus_client import REGISTRY
from prometheus_client import exposition as prometheus_client

from bxcommon.rpc.abstract_rpc_server import CommonRpcServer
from bxcommon.rpc import rpc_constants
from bxgateway.rpc.rpc_request_handler import RpcGatewayRequestHandler

from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


logger = logging.get_logger(__name__)


class GatewayRpcServer(CommonRpcServer):

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self._app.add_routes([web.get("/metrics", self.handle_metrics)])
        self._handler = RpcGatewayRequestHandler(self._node)

    def _authenticate_request(self, request: Request) -> None:
        is_authenticated = True
        if self._encoded_auth is not None:
            # pyre-fixme[16]: Callable `headers` has no attribute `__getitem__`.
            if rpc_constants.AUTHORIZATION_HEADER_KEY in request.headers:
                is_authenticated = self._encoded_auth == request.headers[rpc_constants.AUTHORIZATION_HEADER_KEY]
            else:
                is_authenticated = False
        if not is_authenticated:
            raise HTTPUnauthorized(text="Request credentials are invalid!")

    async def handle_metrics(self, request: Request) -> Response:
        """
        Endpoint for fetching metrics from Prometheus.

        Request can specify a custom encoding type `application/openmetrics-text` to
        fetch data in a different format.
        :param request:
        :return response:
        """
        try:
            self._authenticate_request(request)
            # pyre-fixme[16]: Callable `headers` has no attribute `get`.
            encoder, content_type = prometheus_client.choose_encoder(request.headers.get("Accept"))
            output = encoder(REGISTRY)
            response = Response(
             body=output,
             headers={
                 "Content-Type": content_type
             }
            )
            return response
        except HTTPClientError as e:
            return self._format_client_error(e)
