import os

from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.services.transaction_service import TransactionService
from bxcommon.test_utils import helpers
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash, SHA256_HASH_LEN

from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.services.eth.eth_extension_block_cleanup_service import EthExtensionBlockCleanupService
from bxgateway.testing.abstract_block_cleanup_service_test import AbstractBlockCleanupServiceTest
from bxgateway.services.eth.abstract_eth_block_cleanup_service import AbstractEthBlockCleanupService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService


class EthExtensionBlockCleanupServiceTest(AbstractBlockCleanupServiceTest):
    def setUp(self) -> None:
        super().setUp()
        self.node.block_queuing_service = EthBlockQueuingService(self.node)

    def _get_sample_block(self, file_path):
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(file_path)))
        with open(os.path.join(root_dir, "eth_block_sample.txt")) as sample_file:
            btc_block = sample_file.read().strip("\n")
        buf = bytearray(convert.hex_to_bytes(btc_block))
        parsed_block = NewBlockEthProtocolMessage(msg_bytes=buf)
        return parsed_block

    def _test_mark_blocks_and_request_cleanup(self):
        marked_block = Sha256Hash(binary=helpers.generate_bytearray(SHA256_HASH_LEN))
        prev_block = Sha256Hash(binary=helpers.generate_bytearray(SHA256_HASH_LEN))
        tracked_blocks = []
        self.cleanup_service.on_new_block_received(marked_block, prev_block)
        self.transaction_service.track_seen_short_ids(marked_block, [])
        for _ in range(self.block_confirmations_count - 1):
            tracked_block = Sha256Hash(binary=helpers.generate_bytearray(SHA256_HASH_LEN))
            self.transaction_service.track_seen_short_ids(tracked_block, [])
            tracked_blocks.append(tracked_block)
        unmarked_block = Sha256Hash(binary=helpers.generate_bytearray(SHA256_HASH_LEN))
        self.assertIsNone(self.cleanup_service.last_confirmed_block)
        self.cleanup_service.mark_blocks_and_request_cleanup([marked_block, *tracked_blocks])
        self.assertEqual(marked_block, self.cleanup_service.last_confirmed_block)
        self.assertTrue(self.cleanup_service.is_marked_for_cleanup(marked_block))
        self.assertFalse(self.cleanup_service.is_marked_for_cleanup(unmarked_block))
        self.assertEqual(marked_block, self.cleanup_service.last_confirmed_block)

    def _test_block_cleanup(self):
        block_msg = self._get_sample_block(self._get_file_path())
        transactions = list(block_msg.txns())

        block_hash = block_msg.block_hash()
        transaction_hashes = []

        for idx, tx in enumerate(transactions):
            tx_hash = tx.hash()
            tx_content = str(tx).encode()
            self.transaction_service.set_transaction_contents(tx_hash, tx_content)
            self.transaction_service.assign_short_id(tx_hash, idx + 1)
            transaction_hashes.append(tx_hash)

        self.cleanup_service._block_hash_marked_for_cleanup.add(block_hash)
        self.cleanup_service.clean_block_transactions(block_msg, self.transaction_service)

        self.assertEqual(0, self.transaction_service._total_tx_contents_size)
        for tx_hash in transaction_hashes:
            self.assertFalse(self.transaction_service.has_transaction_contents(tx_hash))

        self.node.post_block_cleanup_tasks.assert_called_once_with(
            block_hash,
            [],
            transaction_hashes
        )

    def test_mark_blocks_and_request_cleanup(self):
        self._test_mark_blocks_and_request_cleanup()

    def test_block_cleanup(self):
        self._test_block_cleanup()

    def test_block_confirmation_cleanup(self):
        self._test_block_confirmation_cleanup()

    def _get_transaction_service(self) -> TransactionService:
        return ExtensionTransactionService(self.node, 1)

    def _get_cleanup_service(self) -> AbstractEthBlockCleanupService:
        return EthExtensionBlockCleanupService(self.node, 1)

    def _get_file_path(self) -> str:
        return __file__
