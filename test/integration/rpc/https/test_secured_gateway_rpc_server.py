from aiohttp import ClientSession
import base64
from bxcommon import constants
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_constants import ContentType
from bxcommon.test_utils import helpers
from bxcommon.test_utils.helpers import async_test
from bxgateway.gateway_opts import GatewayOpts
from bxgateway.rpc.https.gateway_http_rpc_server import GatewayHttpRpcServer
from bxgateway.testing import gateway_helpers
from bxgateway.testing.abstract_gateway_rpc_integration_test import \
    AbstractGatewayRpcIntegrationTest


class SecuredGatewayRpcServerTest(AbstractGatewayRpcIntegrationTest):
    @async_test
    async def setUp(self) -> None:
        self.rpc_port = helpers.get_free_port()
        self.rpc_url = f"http://{constants.LOCALHOST}:{self.rpc_port}"
        self.rpc_user = "user"
        self.rpc_password = "pass"
        await super().setUp()
        self.rpc_server = GatewayHttpRpcServer(self.gateway_node)
        await self.rpc_server.start()

    def get_gateway_opts(self) -> GatewayOpts:
        super().get_gateway_opts()
        opts = gateway_helpers.get_gateway_opts(
            8000,
            rpc_port=self.rpc_port,
            rpc=True,
            rpc_user=self.rpc_user,
            rpc_password=self.rpc_password,
            account_model=self._account_model,
            blockchain_protocol="Ethereum"
        )
        return opts

    async def request(self, req: BxJsonRpcRequest):
        headers = {
            rpc_constants.CONTENT_TYPE_HEADER_KEY: ContentType.JSON.value,
            rpc_constants.AUTHORIZATION_HEADER_KEY: base64.b64encode(
                f"{self.rpc_user}:{self.rpc_password}".encode("utf-8")
            ).decode("utf-8")
        }

        async with ClientSession() as session:
            async with session.post(
                self.rpc_url,
                data=req.to_jsons(),
                headers=headers
            ) as response:
                return JsonRpcResponse.from_json(await response.json())

    @async_test
    async def tearDown(self) -> None:
        await self.rpc_server.stop()
