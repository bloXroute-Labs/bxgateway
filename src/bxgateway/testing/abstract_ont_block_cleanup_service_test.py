import os
import random
import typing
from abc import abstractmethod
from mock import MagicMock

from bxutils import logging
from bxutils.logging.log_level import LogLevel

from bxcommon.services.transaction_service import TransactionService
from bxcommon.test_utils import helpers
from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash

from bxgateway import ont_constants
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.services.ont.abstract_ont_block_cleanup_service import AbstractOntBlockCleanupService
from bxgateway.testing.abstract_block_cleanup_service_test import AbstractBlockCleanupServiceTest
from bxgateway.messages.ont import ont_messages_util

logger = logging.get_logger(__name__)


class AbstractOntBlockCleanupServiceTest(AbstractBlockCleanupServiceTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        logger.setLevel(LogLevel.INFO)

    def _get_sample_block(self, file_path):
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(file_path))))
        with open(os.path.join(root_dir, "samples/ont_sample_block.txt")) as sample_file:
            ont_block = sample_file.read().strip("\n")
        buf = bytearray(convert.hex_to_bytes(ont_block))
        parsed_block = BlockOntMessage(buf=buf)
        return parsed_block

    def _test_mark_blocks_and_request_cleanup(self):
        self.node.has_active_blockchain_peer = MagicMock(return_value=True)
        self.cleanup_service._request_block = MagicMock()

        marked_block = OntObjectHash(binary=helpers.generate_bytearray(ont_constants.ONT_HASH_LEN))
        prev_block = OntObjectHash(binary=helpers.generate_bytearray(ont_constants.ONT_HASH_LEN))
        tracked_blocks = []
        self.cleanup_service.on_new_block_received(marked_block, prev_block)
        self.transaction_service.track_seen_short_ids(marked_block, [])
        for _ in range(self.block_confirmations_count - 1):
            tracked_block = OntObjectHash(binary=helpers.generate_bytearray(ont_constants.ONT_HASH_LEN))
            self.transaction_service.track_seen_short_ids(tracked_block, [])
            tracked_blocks.append(tracked_block)
        unmarked_block = OntObjectHash(binary=helpers.generate_bytearray(ont_constants.ONT_HASH_LEN))
        self.assertIsNone(self.cleanup_service.last_confirmed_block)
        self.cleanup_service.mark_blocks_and_request_cleanup([marked_block, *tracked_blocks])
        self.assertEqual(marked_block, self.cleanup_service.last_confirmed_block)
        self.assertTrue(self.cleanup_service.is_marked_for_cleanup(marked_block))
        self.assertFalse(self.cleanup_service.is_marked_for_cleanup(unmarked_block))
        self.assertEqual(marked_block, self.cleanup_service.last_confirmed_block)
        self.cleanup_service._request_block.assert_called_once()

    def _test_block_cleanup(self):
        block_msg = self._get_sample_block(self._get_file_path())
        transactions = block_msg.txns()
        block_hash = typing.cast(OntObjectHash, block_msg.block_hash())
        random.shuffle(transactions)
        short_len = int(len(transactions) * 0.9)
        transactions_short = transactions[:short_len]
        unknown_transactions = transactions[short_len:]
        transaction_keys = []
        for idx, tx in enumerate(transactions_short):
            tx_hash, _ = ont_messages_util.get_txid(tx)
            transaction_key = self.transaction_service.get_transaction_key(tx_hash)
            transaction_keys.append(transaction_key)
            self.transaction_service.set_transaction_contents_by_key(transaction_key, tx)
            self.transaction_service.assign_short_id_by_key(transaction_key, idx + 1)
        for idx, tx in enumerate(unknown_transactions):
            tx_hash, _= ont_messages_util.get_txid(tx)
            transaction_key = self.transaction_service.get_transaction_key(tx_hash)
            transaction_keys.append(transaction_key)
            if idx % 2 == 0:
                self.transaction_service.set_transaction_contents_by_key(transaction_key, tx)
        self.cleanup_service._block_hash_marked_for_cleanup.add(block_hash)
        self.cleanup_service.clean_block_transactions(block_msg, self.transaction_service)
        self.assertEqual(0, self.transaction_service._total_tx_contents_size)
        for transaction_key in transaction_keys:
            self.assertFalse(self.transaction_service.has_transaction_contents_by_key(transaction_key))

    @abstractmethod
    def _get_transaction_service(self) -> TransactionService:
        pass

    @abstractmethod
    def _get_cleanup_service(self) -> AbstractOntBlockCleanupService:
        pass

    @abstractmethod
    def _get_file_path(self) -> str:
        pass
