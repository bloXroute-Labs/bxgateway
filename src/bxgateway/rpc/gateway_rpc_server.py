from aiohttp.web import Request
from aiohttp.web_exceptions import HTTPUnauthorized

from bxcommon.rpc.abstract_rpc_server import CommonRpcServer
from bxcommon.rpc import rpc_constants

from bxutils import logging


logger = logging.get_logger(__name__)


class GatewayRpcServer(CommonRpcServer):

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
