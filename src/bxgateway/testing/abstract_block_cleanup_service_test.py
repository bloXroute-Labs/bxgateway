from abc import abstractmethod, ABCMeta

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.services.transaction_service import TransactionService
from bxcommon.test_utils import helpers

from bxgateway.services.btc.abstract_btc_block_cleanup_service import AbstractBtcBlockCleanupService
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class AbstractBlockCleanupServiceTest(AbstractTestCase, metaclass=ABCMeta):

    def setUp(self) -> None:
        super(AbstractBlockCleanupServiceTest, self).setUp()
        self.block_confirmations_count = 3
        opts = helpers.get_gateway_opts(
            8000,
            block_confirmations_count=self.block_confirmations_count,
            include_default_btc_args=True
        )
        if opts.use_extensions:
            helpers.set_extensions_parallelism()

        self.node = MockGatewayNode(opts)
        self.transaction_service = self._get_transaction_service()
        self.cleanup_service = self._get_cleanup_service()
        self.node._tx_service = self.transaction_service
        self.node.block_cleanup_service = self.cleanup_service


    @abstractmethod
    def _get_sample_block(self, file_path):
        pass

    @abstractmethod
    def _test_mark_blocks_and_request_cleanup(self):
        pass

    @abstractmethod
    def _test_block_cleanup(self):
        pass

    @abstractmethod
    def _get_sample_block(self, file_path):
        pass

    @abstractmethod
    def _test_mark_blocks_and_request_cleanup(self):
        pass

    @abstractmethod
    def _get_transaction_service(self) -> TransactionService:
        pass

    @abstractmethod
    def _get_cleanup_service(self) -> AbstractBtcBlockCleanupService:
        pass

    @abstractmethod
    def _get_file_path(self) -> str:
        pass
