from bxcommon.services.transaction_service import TransactionService
from bxgateway.services.btc.abstract_btc_block_cleanup_service import AbstractBtcBlockCleanupService
from bxgateway.services.btc.btc_normal_block_cleanup_service import BtcNormalBlockCleanupService
from bxgateway.testing.abstract_btc_block_cleanup_service_test import AbstractBtcBlockCleanupServiceTest


class BtcNormalBlockCleanupServiceTest(AbstractBtcBlockCleanupServiceTest):

    def test_mark_blocks_and_request_cleanup(self):
        self._test_mark_blocks_and_request_cleanup()

    def test_block_cleanup(self):
        self._test_block_cleanup()

    def test_block_confirmation_cleanup(self):
        self._test_block_confirmation_cleanup()

    def _get_transaction_service(self) -> TransactionService:
        return TransactionService(self.node, 1)

    def _get_cleanup_service(self) -> AbstractBtcBlockCleanupService:
        return BtcNormalBlockCleanupService(self.node, 1)

    def _get_file_path(self) -> str:
        return __file__

