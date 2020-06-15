import asyncio
from typing import Dict, Any

import rlp
import websockets

from bxcommon import constants
from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxcommon.utils import convert
from bxgateway.feed.pending_transaction_feed import PendingTransactionFeed
from bxgateway.feed.subscriber import Subscriber
from bxgateway.feed.new_transaction_feed import TransactionFeedEntry
from bxgateway.messages.eth.serializers.transaction import Transaction
from bxgateway.rpc.external.eth_ws_subscriber import EthWsSubscriber
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


def tx_to_eth_rpc_json(transaction: Transaction) -> Dict[str, Any]:
    payload = transaction.to_json()
    for field in {"nonce", "gas", "gas_price", "value"}:
        payload[field] = hex(payload[field])

    payload["gasPrice"] = payload["gas_price"]
    del payload["gas_price"]
    return payload


class EthWsSubscriberTest(AbstractTestCase):
    @async_test
    async def setUp(self) -> None:
        self.eth_ws_port = helpers.get_free_port()
        self.eth_ws_uri = f"ws://127.0.0.1:{self.eth_ws_port}"
        self.eth_ws_server_message_queue = asyncio.Queue()
        self.eth_subscription_id = "sub_id"
        self.eth_test_ws_server = await websockets.serve(
            self.ws_test_serve, constants.LOCALHOST, self.eth_ws_port
        )

        self.gateway_node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000, eth_ws_uri=self.eth_ws_uri)
        )

        self.eth_ws_subscriber = EthWsSubscriber(
            self.eth_ws_uri, self.gateway_node.feed_manager, self.gateway_node.get_tx_service()
        )
        self.subscriber: Subscriber[
            TransactionFeedEntry
        ] = self.gateway_node.feed_manager.subscribe_to_feed(PendingTransactionFeed.NAME)

        await self.eth_ws_subscriber.start()
        await asyncio.sleep(0.01)

        self.assertIsNotNone(self.eth_ws_subscriber.receiving_task)
        self.assertEqual(0, self.subscriber.messages.qsize())

        self.sample_transactions = {
            i: mock_eth_messages.get_dummy_transaction(i) for i in range(10)
        }

    async def ws_test_serve(self, websocket, path):
        async def consumer(ws, _path):
            try:
                async for message in ws:
                    rpc_request = JsonRpcRequest.from_jsons(message)
                    if rpc_request.method_name == "eth_subscribe":
                        await ws.send(
                            JsonRpcResponse(rpc_request.id, self.eth_subscription_id).to_jsons()
                        )
                    elif rpc_request.method_name == "eth_getTransactionByHash":
                        nonce = int(rpc_request.id)
                        await ws.send(
                            JsonRpcResponse(
                                rpc_request.id,
                                tx_to_eth_rpc_json(self.sample_transactions[nonce])
                            ).to_jsons()
                        )
            except Exception as e:
                # server closed, exit
                pass

        async def producer(ws, _path):
            try:
                while True:
                    message = await self.eth_ws_server_message_queue.get()
                    await ws.send(
                        JsonRpcRequest(
                            None,
                            "eth_subscription",
                            {"subscription": self.eth_subscription_id, "result": message},
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

        await self.eth_ws_server_message_queue.put(f"0x{convert.bytes_to_hex(tx_hash.binary)}")
        await self.eth_ws_server_message_queue.put(f"0x{convert.bytes_to_hex(tx_hash_2.binary)}")
        await asyncio.sleep(0.01)

        self.assertEqual(2, self.subscriber.messages.qsize())

        tx_message_1 = await self.subscriber.receive()
        self.assertEqual(convert.bytes_to_hex(tx_hash.binary), tx_message_1.tx_hash)
        self.assertEqual(tx_contents.to_json(), tx_message_1.tx_contents)

        tx_message_2 = await self.subscriber.receive()
        self.assertEqual(convert.bytes_to_hex(tx_hash_2.binary), tx_message_2.tx_hash)
        self.assertEqual(tx_contents_2.to_json(), tx_message_2.tx_contents)

    @async_test
    async def test_subscription_with_no_content_filled(self):
        tx_hash = helpers.generate_hash()
        await self.eth_ws_server_message_queue.put(f"0x{convert.bytes_to_hex(tx_hash)}")

        await asyncio.sleep(0.01)

        self.assertEqual(1, self.subscriber.messages.qsize())
        tx_message = await self.subscriber.receive()
        self.assertEqual(convert.bytes_to_hex(tx_hash), tx_message.tx_hash)

        expected_contents = self.sample_transactions[2].to_json()
        self.assertEqual(expected_contents, tx_message.tx_contents)

    @async_test
    async def tearDown(self) -> None:
        await self.eth_ws_subscriber.stop()
        self.eth_test_ws_server.close()
        await self.eth_test_ws_server.wait_closed()
