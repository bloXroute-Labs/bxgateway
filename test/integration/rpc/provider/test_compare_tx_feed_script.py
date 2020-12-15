import asyncio
import sys
from datetime import date
from typing import Dict, Optional
from asynctest import patch

from bxcommon import constants
from bxcommon.feed.feed import FeedKey
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.services.threaded_request_service import ThreadedRequestService
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_config_base import BdnServiceModelConfigBase
from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed

from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxcommon.feed.new_transaction_feed import FeedSource
from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxgateway.rpc.ws.ws_server import WsServer
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bloxroute_cli import compare_tx_feeds


def reset_compare_tx_feed_script_global_state() -> None:
    compare_tx_feeds.time_to_begin_comparison = 0.0
    compare_tx_feeds.time_to_end_comparison = 0.0
    compare_tx_feeds.trail_new_hashes = set()
    compare_tx_feeds.lead_new_hashes = set()
    compare_tx_feeds.low_fee_hashes = set()
    compare_tx_feeds.seen_hashes = {}
    compare_tx_feeds.num_intervals_passed = 0
    compare_tx_feeds.num_intervals = None


def generate_new_eth_transaction() -> TxMessage:
    transaction = mock_eth_messages.get_dummy_transaction(1, to_address_str="ef26fd0f0f95d28408fc663b1e4de25855ff7f73")
    transactions_eth_message = TransactionsEthProtocolMessage(None, [transaction])
    tx_message = EthNormalMessageConverter().tx_to_bx_txs(transactions_eth_message, 5)[0][0]
    return tx_message


async def mock_process_new_txs_eth(*args) -> None:
    while True:
        await asyncio.sleep(1)


def verify_compare_tx_feed_script_results(
    seen_hashes: Dict[str, compare_tx_feeds.HashEntry], ignore_delta: int, verbose: bool
) -> None:
    assert (len(seen_hashes) > 0)
    reset_compare_tx_feed_script_global_state()
    print(f"{len(seen_hashes)} seen hashes.")


def verify_compare_tx_feed_script_results_exclude_duplicates(
    seen_hashes: Dict[str, compare_tx_feeds.HashEntry], ignore_delta: int, verbose: bool
) -> None:
    assert (len(seen_hashes) == 1)
    reset_compare_tx_feed_script_global_state()


def verify_compare_tx_feed_script_results_exclude_from_blockchain(
    seen_hashes: Dict[str, compare_tx_feeds.HashEntry], ignore_delta: int, verbose: bool
) -> None:
    assert (len(seen_hashes) == 0)
    reset_compare_tx_feed_script_global_state()


class CompareTxFeedScriptTest(AbstractTestCase):
    @async_test
    async def setUp(self) -> None:
        account_model = BdnAccountModelBase(
            "account_id", "account_name", "fake_certificate",
            new_transaction_streaming=BdnServiceModelConfigBase(
                expire_date=date(2999, 1, 1).isoformat()
            )
        )
        gateway_opts = gateway_helpers.get_gateway_opts(8000, ws=True)
        gateway_opts.set_account_options(account_model)

        self.gateway_node = MockGatewayNode(gateway_opts)
        self.gateway_node.NODE_TYPE = NodeType.INTERNAL_GATEWAY
        self.transaction_streamer_peer = OutboundPeerModel("127.0.0.1", 8006, node_type=NodeType.INTERNAL_GATEWAY)
        self.gateway_node.requester = ThreadedRequestService(
            "mock_thread_service",
            self.gateway_node.alarm_queue,
            constants.THREADED_HTTP_POOL_SLEEP_INTERVAL_S
        )
        self.gateway_node.requester.start()
        self.server = WsServer(
            constants.LOCALHOST, 8005, self.gateway_node.feed_manager, self.gateway_node
        )
        self.ws_uri = f"ws://{constants.LOCALHOST}:8005"

        await self.server.start()

    async def send_tx_to_new_txs_feed(self, feed_source: Optional[FeedSource] = FeedSource.BDN_SOCKET):
        await asyncio.sleep(1)
        for _ in range(20):
            eth_tx_message = generate_new_eth_transaction()
            eth_transaction = EthRawTransaction(
                eth_tx_message.tx_hash(),
                eth_tx_message.tx_val(),
                feed_source
            )
            self.gateway_node.feed_manager.publish_to_feed(
                FeedKey("newTxs"), eth_transaction
            )
            await asyncio.sleep(0.03)

    async def send_tx_to_pending_txs_feed(self):
        await asyncio.sleep(1)
        for _ in range(20):
            eth_tx_message = generate_new_eth_transaction()
            eth_transaction = EthRawTransaction(
                eth_tx_message.tx_hash(), eth_tx_message.tx_val(), FeedSource.BDN_SOCKET
            )
            self.gateway_node.feed_manager.publish_to_feed(
                FeedKey("pendingTxs"), eth_transaction
            )
            await asyncio.sleep(0.03)

    async def send_duplicate_tx_to_pending_txs_feed(self):
        eth_tx_message = generate_new_eth_transaction()
        eth_transaction = EthRawTransaction(
            eth_tx_message.tx_hash(), eth_tx_message.tx_val(), FeedSource.BDN_SOCKET
        )
        await asyncio.sleep(1)
        for _ in range(20):
            self.gateway_node.feed_manager.publish_to_feed(
                FeedKey("pendingTxs"), eth_transaction
            )
            await asyncio.sleep(0.03)

    @async_test(3)
    @patch("bloxroute_cli.compare_tx_feeds.stats", verify_compare_tx_feed_script_results)
    async def test_compare_tx_feeds_script_new_txs(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthNewTransactionFeed()
        )

        sys.argv = [
            "main.py",
            "--gateway", "ws://127.0.0.1:8005",
            "--eth", "ws://123.4.5.6:7890",
            "--feed-name", "newTxs",
            "--min-gas-price", "0.000000001",
            "--num-intervals", "1",
            "--interval", "2",
            "--lead-time", "0",
            "--trail-time", "0",
            "--ignore-delta", "5",
            "--addresses", "0xef26fd0f0f95d28408fc663b1e4de25855ff7f73"
        ]
        compare_tx_feeds.process_new_txs_eth = AsyncMock()
        asyncio.create_task(self.send_tx_to_new_txs_feed())
        try:
            await compare_tx_feeds.main()
        except SystemExit:
            print("Script exited normally.")

    @async_test(3)
    @patch("bloxroute_cli.compare_tx_feeds.stats", verify_compare_tx_feed_script_results)
    async def test_compare_tx_feeds_script_pending_txs(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )

        sys.argv = [
            "main.py",
            "--gateway", "ws://127.0.0.1:8005",
            "--eth", "ws://123.4.5.6:7890",
            "--feed-name", "pendingTxs",
            "--num-intervals", "1",
            "--interval", "2",
            "--lead-time", "0",
            "--trail-time", "0",
        ]
        compare_tx_feeds.process_new_txs_eth = AsyncMock()
        asyncio.create_task(self.send_tx_to_pending_txs_feed())
        try:
            await compare_tx_feeds.main()
        except SystemExit:
            print("Script exited normally.")

    @async_test(3)
    @patch("bloxroute_cli.compare_tx_feeds.stats", verify_compare_tx_feed_script_results_exclude_duplicates)
    async def test_compare_tx_feeds_script_pending_txs_exclude_duplicates(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthPendingTransactionFeed(self.gateway_node.alarm_queue)
        )

        sys.argv = [
            "main.py",
            "--gateway", "ws://127.0.0.1:8005",
            "--eth", "ws://123.4.5.6:7890",
            "--feed-name", "pendingTxs",
            "--num-intervals", "1",
            "--interval", "2",
            "--lead-time", "0",
            "--trail-time", "0",
            "--exclude-duplicates"
        ]
        compare_tx_feeds.process_new_txs_eth = AsyncMock()
        asyncio.create_task(self.send_duplicate_tx_to_pending_txs_feed())
        try:
            await compare_tx_feeds.main()
        except SystemExit:
            print("Script exited normally.")

    @async_test(3)
    @patch("bloxroute_cli.compare_tx_feeds.stats", verify_compare_tx_feed_script_results)
    async def test_compare_tx_feeds_script_new_txs_exclude_tx_contents(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthNewTransactionFeed()
        )

        sys.argv = [
            "main.py",
            "--gateway", "ws://127.0.0.1:8005",
            "--num-intervals", "1",
            "--interval", "2",
            "--lead-time", "0",
            "--trail-time", "0",
        ]
        compare_tx_feeds.process_new_txs_eth = AsyncMock()
        asyncio.create_task(self.send_tx_to_new_txs_feed())
        try:
            await compare_tx_feeds.main()
        except SystemExit:
            pass

    @async_test(3)
    @patch("bloxroute_cli.compare_tx_feeds.stats", verify_compare_tx_feed_script_results)
    async def test_compare_tx_feeds_script_new_txs_include_from_blockchain_default(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthNewTransactionFeed()
        )

        sys.argv = [
            "main.py",
            "--gateway", "ws://127.0.0.1:8005",
            "--num-intervals", "1",
            "--interval", "2",
            "--lead-time", "0",
            "--trail-time", "0"
        ]
        compare_tx_feeds.process_new_txs_eth = AsyncMock()
        asyncio.create_task(self.send_tx_to_new_txs_feed(FeedSource.BLOCKCHAIN_SOCKET))
        try:
            await compare_tx_feeds.main()
        except SystemExit:
            pass

    @async_test(3)
    @patch("bloxroute_cli.compare_tx_feeds.stats", verify_compare_tx_feed_script_results_exclude_from_blockchain)
    async def test_compare_tx_feeds_script_new_txs_exclude_from_blockchain(self):
        self.gateway_node.feed_manager.feeds.clear()
        self.gateway_node.feed_manager.register_feed(
            EthNewTransactionFeed()
        )

        sys.argv = [
            "main.py",
            "--gateway", "ws://127.0.0.1:8005",
            "--num-intervals", "1",
            "--interval", "2",
            "--lead-time", "0",
            "--trail-time", "0",
            "--exclude-from-blockchain"
        ]
        compare_tx_feeds.process_new_txs_eth = AsyncMock()
        asyncio.create_task(self.send_tx_to_new_txs_feed(FeedSource.BLOCKCHAIN_SOCKET))
        try:
            await compare_tx_feeds.main()
        except SystemExit:
            pass

    @async_test
    async def tearDown(self) -> None:
        await self.server.stop()
