import asyncio

from mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxgateway.feed.new_transaction_feed import FeedSource
from bxgateway.testing.mocks import mock_eth_messages


class EthNewTransactionFeedTest(AbstractTestCase):

    def setUp(self) -> None:
        self.sut = EthNewTransactionFeed()

    @async_test
    async def test_publish_from_blockchain(self):
        normal_subscriber = self.sut.subscribe({})
        no_from_bc_subscriber = self.sut.subscribe({"include_from_blockchain": False})

        self.sut.publish(
            mock_eth_messages.generate_eth_raw_transaction(
                FeedSource.BLOCKCHAIN_SOCKET
            )
        )

        result = await asyncio.wait_for(normal_subscriber.receive(), 0.01)
        self.assertIsNotNone(result)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(no_from_bc_subscriber.receive(), 0.01)

    @async_test
    async def test_publish_from_blockchain_skips_publish_no_subscribers(self):
        self.sut.subscribe({"include_from_blockchain": False})
        self.sut.serialize = MagicMock(wraps=self.sut.serialize)

        self.sut.publish(
            mock_eth_messages.generate_eth_raw_transaction(FeedSource.BLOCKCHAIN_SOCKET)
        )

        self.sut.serialize.assert_not_called()
