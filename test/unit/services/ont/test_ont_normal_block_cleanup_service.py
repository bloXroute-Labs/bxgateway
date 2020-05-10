from unittest import skip

from bxcommon.services.transaction_service import TransactionService
from bxgateway.services.ont.abstract_ont_block_cleanup_service import AbstractOntBlockCleanupService
from bxgateway.services.ont.ont_normal_block_cleanup_service import OntNormalBlockCleanupService
from bxgateway.testing.abstract_ont_block_cleanup_service_test import AbstractOntBlockCleanupServiceTest


@skip("not yet implemented")
class OntNormalBlockCleanupServiceTest(AbstractOntBlockCleanupServiceTest):

    def test_mark_blocks_and_request_cleanup(self):
        self._test_mark_blocks_and_request_cleanup()

    def test_block_cleanup(self):
        self._test_block_cleanup()

    def test_block_confirmation_cleanup(self):
        self._test_block_confirmation_cleanup()

    def _get_transaction_service(self) -> TransactionService:
        return TransactionService(self.node, 1)

    def _get_cleanup_service(self) -> AbstractOntBlockCleanupService:
        return OntNormalBlockCleanupService(self.node, 1)

    def _get_file_path(self) -> str:
        return __file__

