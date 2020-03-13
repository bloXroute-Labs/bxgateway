import json
from typing import Type, Callable, Awaitable
from aiohttp.web_exceptions import HTTPBadRequest, HTTPAccepted
from mock import MagicMock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxcommon.utils import convert

from bxgateway.testing.mocks.mock_rpc_request import MockRPCRequest
from bxgateway.rpc.rpc_request_handler import RPCRequestHandler

MISSING_CONTENT_TYPE_REQUEST = MockRPCRequest(headers={}, json_body="")
BAD_CONTENT_TYPE_REQUEST = MockRPCRequest(headers={RPCRequestHandler.CONTENT_TYPE: "application/json"}, json_body="")
BAD_JSON_BODY_REQUEST = MockRPCRequest(
    headers={RPCRequestHandler.CONTENT_TYPE: RPCRequestHandler.PLAIN},
    json_body="bad json: bad data"
)
BAD_RPC_REQUEST = MockRPCRequest(
    headers={RPCRequestHandler.CONTENT_TYPE: RPCRequestHandler.PLAIN},
    json_body=json.dumps(dict(
        method="not_valid_method",
        id="some id"
    ))
)
OK_RPC_REQUEST = MockRPCRequest(
    headers={RPCRequestHandler.CONTENT_TYPE: RPCRequestHandler.PLAIN},
    json_body=json.dumps(dict(
        method="blxr_tx",
        id="some id",
        params=dict(transaction=convert.bytes_to_hex(helpers.generate_bytes(150)))
    ))
)


async def test_raises(error_type: Type[Exception], callback: Callable[..., Awaitable], *args, **kwargs):
    try:
        await callback(*args, **kwargs)
    except error_type:
        pass


class MockBlxrTxRequest(AsyncMock):

    async def process_request(self):
        self.mock.process_request()
        return HTTPAccepted()


class RPCRequestHandlerTest(AbstractTestCase):

    def setUp(self) -> None:
        self.handler = RPCRequestHandler(MagicMock())
        self.blxr_tx_request_mock = MockBlxrTxRequest()

        def get_rpc_request(hdlr, request_type):
            return self.blxr_tx_request_mock

        self.handler._get_rpc_request = get_rpc_request

    @async_test
    async def test_handle_bad_requests(self):
        await test_raises(HTTPBadRequest, self.handler.handle_request, MISSING_CONTENT_TYPE_REQUEST)
        await test_raises(HTTPBadRequest, self.handler.handle_request, BAD_CONTENT_TYPE_REQUEST)
        await test_raises(HTTPBadRequest, self.handler.handle_request, BAD_JSON_BODY_REQUEST)
        await test_raises(HTTPBadRequest, self.handler.handle_request, BAD_RPC_REQUEST)
        await self.handler.handle_request(OK_RPC_REQUEST)
        self.blxr_tx_request_mock.mock.process_request.assert_called_once()

