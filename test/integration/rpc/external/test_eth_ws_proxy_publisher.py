import asyncio
import rlp
import websockets

from typing import Dict, Any
from datetime import date
from unittest.mock import patch
from mock import MagicMock

from bxcommon import constants
from bxcommon.feed.feed import FeedKey
from bxcommon.models.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_config_base import BdnServiceModelConfigBase

from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxcommon.feed.subscriber import Subscriber
from bxcommon.feed.new_transaction_feed import RawTransactionFeedEntry
from bxgateway.rpc.external.eth_ws_proxy_publisher import EthWsProxyPublisher
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


TX_SUB_ID = "newPendingTransactions"
_recover_public_key = crypto_utils.recover_public_key


def tx_to_eth_rpc_json(transaction: Transaction) -> Dict[str, Any]:
    payload = transaction.to_json()
    payload["gasPrice"] = payload["gas_price"]
    del payload["gas_price"]
    return payload


class EthWsProxyPublisherTest(AbstractTestCase):
    @async_test
    async def setUp(self) -> None:
        crypto_utils.recover_public_key = MagicMock(
            return_value=bytes(32))
        account_model = BdnAccountModelBase(
            "account_id", "account_name", "fake_certificate",
            new_transaction_streaming=BdnServiceModelConfigBase(
                expire_date=date(2999, 1, 1).isoformat()
            )
        )

        self.eth_ws_port = helpers.get_free_port()
        self.eth_ws_uri = f"ws://127.0.0.1:{self.eth_ws_port}"
        self.eth_ws_server_message_queue = asyncio.Queue()
        await self.start_server()

        gateway_opts = gateway_helpers.get_gateway_opts(
            8000, eth_ws_uri=self.eth_ws_uri, ws=True)
        gateway_opts.set_account_options(account_model)
        self.gateway_node = MockGatewayNode(gateway_opts)
        self.gateway_node.transaction_streamer_peer = OutboundPeerModel("127.0.0.1", 8006, node_type=NodeType.INTERNAL_GATEWAY)
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )

        self.eth_ws_proxy_publisher = EthWsProxyPublisher(
            self.eth_ws_uri, self.gateway_node.feed_manager, self.gateway_node.get_tx_service(), self.gateway_node
        )
        self.subscriber: Subscriber[
            RawTransactionFeedEntry
        ] = self.gateway_node.feed_manager.subscribe_to_feed(
            FeedKey(EthPendingTransactionFeed.NAME), {})
        self.assertIsNotNone(self.subscriber)

        await self.eth_ws_proxy_publisher.start()
        await asyncio.sleep(0.01)

        self.assertEqual(len(self.eth_ws_proxy_publisher.receiving_tasks), 2)
        self.assertEqual(0, self.subscriber.messages.qsize())

        self.sample_transactions = {
            i: mock_eth_messages.get_dummy_transaction(i) for i in range(10)
        }

    async def start_server(self) -> None:
        self.eth_test_ws_server = await websockets.serve(
            self.ws_test_serve, constants.LOCALHOST, self.eth_ws_port
        )

    async def ws_test_serve(self, websocket, path):
        async def consumer(ws, _path):
            try:
                async for message in ws:
                    rpc_request = JsonRpcRequest.from_jsons(message)
                    if rpc_request.method_name == "eth_subscribe":
                        await ws.send(
                            JsonRpcResponse(rpc_request.id, str(rpc_request.params[0])).to_jsons()
                        )
                    elif rpc_request.method_name == "eth_getTransactionByHash":
                        nonce = int(rpc_request.id)
                        await ws.send(
                            JsonRpcResponse(
                                rpc_request.id,
                                tx_to_eth_rpc_json(self.sample_transactions[nonce])
                            ).to_jsons()
                        )
            except Exception:
                # server closed, exit
                pass

        async def producer(ws, _path):
            try:
                while True:
                    subscription, message = await self.eth_ws_server_message_queue.get()
                    await ws.send(
                        JsonRpcRequest(
                            None,
                            "eth_subscription",
                            {"subscription": subscription, "result": message},
                        ).to_jsons()
                    )
            except Exception:
                # server closed, exit
                pass

        consumer_task = asyncio.create_task(consumer(websocket, path))
        producer_task = asyncio.create_task(producer(websocket, path))
        done, pending = await asyncio.wait(
            [consumer_task, producer_task], return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()

    @async_test
    async def test_subscription(self):
        tx_hash = helpers.generate_object_hash()
        tx_contents = mock_eth_messages.get_dummy_transaction(1)
        self.gateway_node.get_tx_service().set_transaction_contents(
            tx_hash,
            rlp.encode(tx_contents)
        )

        tx_hash_2 = helpers.generate_object_hash()
        tx_contents_2 = mock_eth_messages.get_dummy_transaction(2)
        self.gateway_node.get_tx_service().set_transaction_contents(
            tx_hash_2,
            rlp.encode(tx_contents_2)
        )

        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash.binary)}"))
        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash_2.binary)}"))
        await asyncio.sleep(0.01)

        self.assertEqual(2, self.subscriber.messages.qsize())

        tx_message_1 = await self.subscriber.receive()
        self.assertEqual(f"0x{convert.bytes_to_hex(tx_hash.binary)}", tx_message_1["tx_hash"])
        self.assertEqual(tx_contents.to_json(), tx_message_1["tx_contents"])

        tx_message_2 = await self.subscriber.receive()
        self.assertEqual(f"0x{convert.bytes_to_hex(tx_hash_2.binary)}", tx_message_2["tx_hash"])
        self.assertEqual(tx_contents_2.to_json(), tx_message_2["tx_contents"])

    @async_test
    async def test_subscription_with_no_content_filled(self):
        self.maxDiff = None
        tx_hash = helpers.generate_hash()
        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash)}"))

        await asyncio.sleep(0.01)

        self.assertEqual(1, self.subscriber.messages.qsize())
        tx_message = await self.subscriber.receive()
        self.assertEqual(f"0x{convert.bytes_to_hex(tx_hash)}", tx_message["tx_hash"])

        expected_contents = self.sample_transactions[3].to_json()
        self.assertEqual(expected_contents, tx_message["tx_contents"])

    @patch("bxcommon.constants.WS_RECONNECT_TIMEOUTS", [0.01])
    @patch("bxcommon.constants.WS_MIN_RECONNECT_TIMEOUT_S", 0)
    @async_test
    async def test_disconnect_server(self):
        tx_hash = helpers.generate_object_hash()
        tx_contents = mock_eth_messages.get_dummy_transaction(1)
        self.gateway_node.get_tx_service().set_transaction_contents(
            tx_hash,
            rlp.encode(tx_contents)
        )

        self.eth_test_ws_server.close()
        await self.eth_test_ws_server.wait_closed()
        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash.binary)}"))

        await asyncio.sleep(0.05)

        self.assertEqual(0, self.subscriber.messages.qsize())
        self.assertFalse(self.eth_ws_proxy_publisher.running)

    @patch("bxcommon.constants.WS_RECONNECT_TIMEOUTS", [0.01, 0.05])
    @patch("bxcommon.constants.WS_MIN_RECONNECT_TIMEOUT_S", 0)
    @async_test
    async def test_disconnect_server_reconnect(self):
        tx_hash = helpers.generate_object_hash()
        tx_contents = mock_eth_messages.get_dummy_transaction(1)
        self.gateway_node.get_tx_service().set_transaction_contents(
            tx_hash,
            rlp.encode(tx_contents)
        )

        self.eth_test_ws_server.close()
        await self.eth_test_ws_server.wait_closed()
        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash.binary)}"))

        await asyncio.sleep(0.02)

        self.assertEqual(0, self.subscriber.messages.qsize())
        self.assertTrue(self.eth_ws_proxy_publisher.running)
        self.assertFalse(self.eth_ws_proxy_publisher.connected_event.is_set())

        await self.start_server()
        await asyncio.sleep(0.1)

        self.assertTrue(self.eth_ws_proxy_publisher.connected_event.is_set())
        self.assertEqual(0, self.subscriber.messages.qsize())

        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash.binary)}"))
        await asyncio.sleep(0.01)
        self.assertEqual(1, self.subscriber.messages.qsize())

    @patch("bxcommon.constants.WS_RECONNECT_TIMEOUTS", [0.01])
    @patch("bxcommon.constants.WS_MIN_RECONNECT_TIMEOUT_S", 0)
    @async_test
    async def test_disconnect_server_revive(self):
        tx_hash = helpers.generate_object_hash()
        tx_contents = mock_eth_messages.get_dummy_transaction(1)
        self.gateway_node.get_tx_service().set_transaction_contents(
            tx_hash,
            rlp.encode(tx_contents)
        )

        self.eth_test_ws_server.close()
        await self.eth_test_ws_server.wait_closed()
        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash.binary)}"))

        await asyncio.sleep(0.05)

        self.assertEqual(0, self.subscriber.messages.qsize())
        self.assertFalse(self.eth_ws_proxy_publisher.running)

        await self.start_server()
        await asyncio.sleep(0)

        await self.eth_ws_proxy_publisher.revive()
        await self.eth_ws_server_message_queue.put((TX_SUB_ID, f"0x{convert.bytes_to_hex(tx_hash.binary)}"))
        await asyncio.sleep(0.01)
        self.assertEqual(1, self.subscriber.messages.qsize())

    @async_test
    async def tearDown(self) -> None:
        await self.eth_ws_proxy_publisher.stop()
        self.eth_test_ws_server.close()
        await self.eth_test_ws_server.wait_closed()
        crypto_utils.recover_public_key = _recover_public_key
