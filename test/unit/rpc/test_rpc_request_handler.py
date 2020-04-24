import json
from typing import Type, Callable, Awaitable
from aiohttp.web_exceptions import HTTPBadRequest, HTTPAccepted
from mock import MagicMock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxcommon.utils import convert

from bxgateway.testing.mocks.mock_rpc_request import MockRPCRequest
from bxgateway.rpc.rpc_request_handler import RpcGatewayRequestHandler

from bxgateway.messages.eth import eth_message_converter
from bxgateway.messages.btc import btc_normal_message_converter


MISSING_CONTENT_TYPE_REQUEST = MockRPCRequest(headers={}, json_body="")
BAD_CONTENT_TYPE_REQUEST = MockRPCRequest(headers={RpcGatewayRequestHandler.CONTENT_TYPE: "application/json"}, json_body="")
BAD_JSON_BODY_REQUEST = MockRPCRequest(
    headers={RpcGatewayRequestHandler.CONTENT_TYPE: RpcGatewayRequestHandler.PLAIN},
    json_body="bad json: bad data"
)
BAD_RPC_REQUEST = MockRPCRequest(
    headers={RpcGatewayRequestHandler.CONTENT_TYPE: RpcGatewayRequestHandler.PLAIN},
    json_body=json.dumps(dict(
        method="not_valid_method",
        id="some id"
    ))
)
OK_RPC_REQUEST = MockRPCRequest(
    headers={RpcGatewayRequestHandler.CONTENT_TYPE: RpcGatewayRequestHandler.PLAIN},
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


class RpcGatewayRequestHandlerTest(AbstractTestCase):

    def setUp(self) -> None:
        self.handler = RpcGatewayRequestHandler(MagicMock())
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


class RpcEthTransactionConversionTest(AbstractTestCase):
    def setUp(self) -> None:
        self.test_tx_str = "f86b01847735940082520894a2f6090c0483d6e6ac90a9c23d42e461fee2ac5188016147191f13b0008025a0784537f9801331b707ceedd5388d318d86b0bb43c6f5b32b30c9df960f596b05a042fe22aa47f2ae80cbb2c9272df2f8975c96a8a99020d8ac19d4d4b0e0b58219"
        self.message_convertor = eth_message_converter.EthMessageConverter()

    def test_eth_tx_convertor_hex_str(self):

        # msg payload (tx value) equals source
        tx_bytes = self.message_convertor.encode_raw_msg(self.test_tx_str)
        msg = self.message_convertor.bdn_tx_to_bx_tx(tx_bytes, 2)
        self.assertEqual(msg.tx_val().hex(), self.test_tx_str)

        # bytes / bytearray input most contain the payload as bytes and not hex string
        with self.assertRaises(eth_message_converter.ParseError):
            self.message_convertor.bdn_tx_to_bx_tx(self.test_tx_str.encode(), 2)


class RpcBtcTransactionConversionTest(AbstractTestCase):
    def setUp(self) -> None:
        self.test_tx_str = "01000000017b1eabe0209b1fe794124575ef807057c77ada2138ae4fa8d6c4de0398a14f3f0000000000ffffffff01f0ca052a010000001976a914cbc20a7664f2f69e5355aa427045bc15e7c6c77288ac00000000"
        self.message_convertor = btc_normal_message_converter.BtcNormalMessageConverter(btc_magic=0x1234)

    def test_btc_tx_convertor_hex_str(self):

        # msg payload (tx value) equals source
        tx_bytes = self.message_convertor.encode_raw_msg(self.test_tx_str)
        msg = self.message_convertor.bdn_tx_to_bx_tx(tx_bytes, 1)
        self.assertEqual(msg.tx_val().hex(), self.test_tx_str)

        # bytes / bytearray input most contain the payload as bytes and not hex string
        with self.assertRaises(ValueError):
            self.message_convertor.bdn_tx_to_bx_tx(self.test_tx_str.encode(), 1)
