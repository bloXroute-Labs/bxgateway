from abc import abstractmethod, ABCMeta

from mock import MagicMock

from bxgateway.testing import gateway_helpers
from bxcommon.messages.bloxroute.block_confirmation_message import BlockConfirmationMessage
from bxcommon.services.transaction_service import TransactionService
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class AbstractBlockCleanupServiceTest(AbstractTestCase, metaclass=ABCMeta):

    def setUp(self) -> None:
        self.block_confirmations_count = 3
        opts = gateway_helpers.get_gateway_opts(
            8000,
            block_confirmations_count=self.block_confirmations_count - 1,
            include_default_btc_args=True
        )
        if opts.use_extensions:
            helpers.set_extensions_parallelism()

        self.node = MockGatewayNode(opts)
        self.transaction_service = self._get_transaction_service()
        self.cleanup_service = self._get_cleanup_service()
        self.node._tx_service = self.transaction_service
        self.node.block_cleanup_service = self.cleanup_service
        self.node.post_block_cleanup_tasks = MagicMock()
        self.tx_hashes = []

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
    def _get_cleanup_service(self) -> AbstractBlockCleanupService:
        pass

    @abstractmethod
    def _get_file_path(self) -> str:
        pass

    def _get_block_confirmation_msg(self):
        short_ids_len = 10
        block_hash = Sha256Hash(helpers.generate_bytearray(SHA256_HASH_LEN))
        tx_hashes = [Sha256Hash(helpers.generate_bytearray(SHA256_HASH_LEN)) for _ in range(short_ids_len * 2)]
        self.tx_hashes = tx_hashes
        short_ids = list(range(1, short_ids_len + 1))
        network_num = 4
        confirmation_message = BlockConfirmationMessage(
            sids=short_ids,
            tx_hashes=tx_hashes,
            message_hash=block_hash,
            network_num=network_num
        )
        return confirmation_message

    def _test_block_confirmation_cleanup(self):
        confirmation_message = self._get_block_confirmation_msg()
        self.cleanup_service.contents_cleanup(self.transaction_service, confirmation_message)
        for idx, tx_hash in enumerate(self.tx_hashes):
            self.assertFalse(self.transaction_service.has_transaction_contents(tx_hash))
            self.assertFalse(self.transaction_service.has_short_id(idx + 1))
        self.assertEqual(0, self.transaction_service._total_tx_contents_size)
