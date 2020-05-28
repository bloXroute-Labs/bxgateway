import asyncio

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
from bxgateway.feed.unconfirmed_transaction_feed import TransactionFeedEntry
from bxgateway.rpc.external.eth_ws_subscriber import EthWsSubscriber
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


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

    async def ws_test_serve(self, websocket, path):
        async def consumer(ws, _path):
            try:
                async for message in ws:
                    rpc_request = JsonRpcRequest.from_jsons(message)
                    await ws.send(
                        JsonRpcResponse(rpc_request.id, self.eth_subscription_id).to_jsons()
                    )
            except Exception:
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
        subscriber: Subscriber[
            TransactionFeedEntry
        ] = self.gateway_node.feed_manager.subscribe_to_feed(PendingTransactionFeed.NAME)

        await self.eth_ws_subscriber.start()
        await asyncio.sleep(0.01)
        self.assertIsNotNone(self.eth_ws_subscriber.ws_client)
        self.assertIsNotNone(self.eth_ws_subscriber.receiving_task)
        self.assertEqual(self.eth_subscription_id, self.eth_ws_subscriber.subscription_id)

        self.assertEqual(0, subscriber.messages.qsize())

        tx_hash = helpers.generate_object_hash()
        tx_contents = helpers.generate_bytearray(250)
        self.gateway_node.get_tx_service().set_transaction_contents(tx_hash, tx_contents)
        tx_hash_2 = helpers.generate_hash()

        await self.eth_ws_server_message_queue.put(f"0x{convert.bytes_to_hex(tx_hash.binary)}")
        await self.eth_ws_server_message_queue.put(f"0x{convert.bytes_to_hex(tx_hash_2)}")
        await asyncio.sleep(0.01)

        self.assertEqual(2, subscriber.messages.qsize())

        tx_message_1 = await subscriber.receive()
        self.assertEqual(convert.bytes_to_hex(tx_hash.binary), tx_message_1.tx_hash)
        self.assertEqual(convert.bytes_to_hex(tx_contents), tx_message_1.tx_contents)

        tx_message_2 = await subscriber.receive()
        self.assertEqual(convert.bytes_to_hex(tx_hash_2), tx_message_2.tx_hash)
        self.assertEqual("", tx_message_2.tx_contents)

    @async_test
    async def tearDown(self) -> None:
        await self.eth_ws_subscriber.stop()
        self.eth_test_ws_server.close()
        await self.eth_test_ws_server.wait_closed()
