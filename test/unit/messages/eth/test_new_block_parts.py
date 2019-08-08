import rlp

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody
from bxgateway.testing.mocks import mock_eth_messages


class EthMessagesTests(AbstractTestCase):
    def test_new_block_parts(self):
        txs = []
        txs_bytes = []
        txs_hashes = []

        tx_count = 10

        for i in range(1, tx_count):
            tx = mock_eth_messages.get_dummy_transaction(1)
            txs.append(tx)

            tx_bytes = rlp.encode(tx, Transaction)
            txs_bytes.append(tx_bytes)

            tx_hash = tx.hash()
            txs_hashes.append(tx_hash)

        block_header = mock_eth_messages.get_dummy_block_header(1)

        uncles = [
            mock_eth_messages.get_dummy_block_header(2),
            mock_eth_messages.get_dummy_block_header(3),
        ]

        block_number = 100000

        block_body = TransientBlockBody(txs, uncles)

        block_header_bytes = memoryview(rlp.encode(BlockHeader.serialize(block_header)))
        block_body_bytes = memoryview(rlp.encode(TransientBlockBody.serialize(block_body)))

        new_block_parts = NewBlockParts(block_header_bytes, block_body_bytes, block_number)

        self.assertIsInstance(new_block_parts.get_block_hash(), Sha256Hash)
        self.assertIsInstance(new_block_parts.get_previous_block_hash(), Sha256Hash)
        self.assertEqual(1, new_block_parts.get_block_difficulty())
