import base64
import json

from aiohttp.web_exceptions import HTTPUnauthorized, HTTPOk

from bxgateway.testing import gateway_helpers
from bxcommon.rpc import rpc_constants
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.rpc.https.gateway_http_rpc_server import GatewayHttpRpcServer
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.testing.mocks.mock_rpc_request import MockRpcRequest


# noinspection PyTypeChecker
class GatewayRpcServerTest(AbstractTestCase):
    def setUp(self) -> None:
        self.mock_gateway = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000, rpc_user="user", rpc_password="password")
        )
        self.rpc_server = GatewayHttpRpcServer(self.mock_gateway)
        self.encoded_auth = base64.b64encode("user:password".encode("utf-8")).decode("utf-8")

    @async_test
    async def test_authentication(self):
        unauthorized = await self.rpc_server.handle_request(MockRpcRequest({}, json.dumps({})))
        self.assertEqual(HTTPUnauthorized.status_code, unauthorized.status)

        authorized = await self.rpc_server.handle_request(
            MockRpcRequest(
                {rpc_constants.AUTHORIZATION_HEADER_KEY: self.encoded_auth}, json.dumps({})
            )
        )
        self.assertNotEqual(HTTPUnauthorized.status_code, authorized.status)

    @async_test
    async def test_prometheus_request(self):
        response = await self.rpc_server.handle_metrics(
            MockRpcRequest(
                {
                    rpc_constants.CONTENT_TYPE_HEADER_KEY: rpc_constants.PLAIN_HEADER_TYPE,
                    rpc_constants.AUTHORIZATION_HEADER_KEY: self.encoded_auth,
                },
                json.dumps({}),
            )
        )
        self.assertEqual(HTTPOk.status_code, response.status)
