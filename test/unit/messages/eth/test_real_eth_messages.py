from unittest import skip

from bxcommon.messages.eth.serializers.transaction import AccessListTransaction, LegacyTransaction
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import (
    TransactionsEthProtocolMessage,
)
from bxgateway.testing.fixture import eth_fixtures


class TestRealEthMessages(AbstractTestCase):
    @skip(
        "we do not currently have support for this particular message; test to see if they are all encoded like this"
    )
    def test_open_ethereum_berlin_txs_message(self):
        msg = TransactionsEthProtocolMessage(eth_fixtures.OPEN_ETH_BERLIN_TXS_MESSAGE)
        txs = msg.get_transactions()

        self.assertEqual(56, len(txs))
        for i, tx in enumerate(txs):
            if i == 28:
                self.assertIsInstance(tx, AccessListTransaction)
            else:
                self.assertIsInstance(tx, LegacyTransaction)
