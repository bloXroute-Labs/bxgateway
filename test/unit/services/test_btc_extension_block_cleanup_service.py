import time
from bxcommon.services.extension_transaction_service import ExtensionTransactionService, TransactionService
from bxcommon.test_utils import helpers

from bxgateway.services.btc.abstract_btc_block_cleanup_service import AbstractBtcBlockCleanupService
from bxgateway.services.btc.btc_extension_block_cleanup_service import BtcExtensionBlockCleanupService
from bxgateway.testing.abstract_btc_block_cleanup_service_test import AbstractBtcBlockCleanupServiceTest


class BtcExtensionBlockCleanupServiceTest(AbstractBtcBlockCleanupServiceTest):

    def setUp(self) -> None:
        helpers.set_extensions_parallelism()
        super(BtcExtensionBlockCleanupServiceTest, self).setUp()

    def test_mark_blocks_and_request_cleanup(self):
        self._test_mark_blocks_and_request_cleanup()

    def test_block_cleanup(self):
        self._test_block_cleanup()

    def _get_transaction_service(self) -> TransactionService:
        return ExtensionTransactionService(self.node, 1)

    def _get_cleanup_service(self) -> AbstractBtcBlockCleanupService:
        return BtcExtensionBlockCleanupService(self.node, 1)

    def _get_file_path(self) -> str:
        return __file__
