import asyncio

from mock import MagicMock

from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.fixture import eth_fixtures
from bxcommon.test_utils.helpers import async_test
from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxcommon.feed.new_transaction_feed import FeedSource
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.testing.mocks import mock_eth_messages


class EthNewTransactionFeedTest(AbstractTestCase):

    def setUp(self) -> None:
        self.sut = EthNewTransactionFeed()

    @async_test
    async def test_publish_from_blockchain(self):
        normal_subscriber = self.sut.subscribe({"include_from_blockchain": False})
        yes_from_bc_subscriber = self.sut.subscribe({"include_from_blockchain": True})

        self.sut.publish(
            mock_eth_messages.generate_eth_raw_transaction(
                FeedSource.BLOCKCHAIN_SOCKET
            )
        )

        result = await asyncio.wait_for(yes_from_bc_subscriber.receive(), 0.01)
        self.assertIsNotNone(result)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(normal_subscriber.receive(), 0.01)

    @async_test
    async def test_subscriber_filters_type_compatibility(self):
        # (dynamic fee transaction has a fee of 15)
        subscriber = self.sut.subscribe({"filters": "max_priority_fee_per_gas > 10"})
        subscriber2 = self.sut.subscribe({"filters": "max_priority_fee_per_gas > 100"})
        subscriber3 = self.sut.subscribe({"filters": "gas_price > 0"})

        self.sut.publish(
            EthRawTransaction(
                Sha256Hash.from_string(eth_fixtures.LEGACY_TRANSACTION_HASH),
                memoryview(eth_fixtures.LEGACY_TRANSACTION),
                FeedSource.BLOCKCHAIN_SOCKET,
                True,
            )
        )
        self.sut.publish(
            EthRawTransaction(
                Sha256Hash.from_string(eth_fixtures.DYNAMIC_FEE_TRANSACTION_HASH),
                memoryview(eth_fixtures.DYNAMIC_FEE_TRANSACTION),
                FeedSource.BLOCKCHAIN_SOCKET,
                True,
            )
        )

        result = await asyncio.wait_for(subscriber.receive(), 0.01)
        self.assertIsNotNone(result)
        self.assertEqual(eth_fixtures.DYNAMIC_FEE_TRANSACTION_HASH, result["tx_hash"][2:])

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(subscriber.receive(), 0.01)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(subscriber2.receive(), 0.01)

        # sub3 only gets legacy tx
        result = await asyncio.wait_for(subscriber3.receive(), 0.01)
        self.assertIsNotNone(result)
        self.assertEqual(eth_fixtures.LEGACY_TRANSACTION_HASH, result["tx_hash"][2:])

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(subscriber3.receive(), 0.01)

    @async_test
    async def test_publish_from_blockchain_skips_publish_no_subscribers(self):
        self.sut.subscribe({"include_from_blockchain": False})
        self.sut.serialize = MagicMock(wraps=self.sut.serialize)

        self.sut.publish(
            mock_eth_messages.generate_eth_raw_transaction(FeedSource.BLOCKCHAIN_SOCKET)
        )

        self.sut.serialize.assert_not_called()
