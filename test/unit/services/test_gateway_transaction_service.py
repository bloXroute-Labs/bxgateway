from mock import MagicMock

from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxcommon.test_utils.abstract_transaction_service_test_case import AbstractTransactionServiceTestCase


class GatewayTransactionServiceTest(AbstractTransactionServiceTestCase):
    def setUp(self) -> None:
        super(GatewayTransactionServiceTest, self).setUp()
        self.mock_node.log_txs_network_content = MagicMock()
        self.mock_node.block_recovery_service = MagicMock()


    def test_get_missing_transactions(self):
        self._test_get_missing_transactions()

    def test_sid_assignment_basic(self):
        self._test_sid_assignment_basic()

    def test_sid_assignment_multiple_sids(self):
        self._test_sid_assignment_multiple_sids()

    def test_sid_expiration(self):
        self._test_sid_expiration()

    def test_expire_old_assignments(self):
        self._test_expire_old_assignments()

    def test_sid_expiration_multiple_sids(self):
        self._test_sid_expiration_multiple_sids()

    def test_track_short_ids_seen_in_block(self):
        self._test_track_short_ids_seen_in_block()

    def test_transactions_contents_memory_limit(self):
        self._test_transactions_contents_memory_limit()

    def test_track_short_ids_seen_in_block_multiple_per_tx(self):
        self._test_track_short_ids_seen_in_block_multiple_per_tx()

    def test_verify_tx_removal_by_hash(self):
        self._test_verify_tx_removal_by_hash()

    def test_verify_tx_removal_by_hash_flagged_txs(self):
        self._test_verify_tx_removal_by_hash_flagged_txs()

    def test_memory_stats(self):
        self._test_memory_stats()

    def test_iter_timestamped_transaction_hashes_from_oldest(self):
        self._test_iter_transaction_hashes_from_oldest()

    def test_add_tx_without_sid(self):
        self._test_add_tx_without_sid()

    def test_add_tx_without_sid_expire(self):
        self._test_add_tx_without_sid_expire()

    def test_add_sid_without_content(self):
        self._test_add_sid_without_content()

    def test_process_gateway_transaction_from_bdn(self):
        self._test_process_gateway_transaction_from_bdn()

    def test_get_transactions(self):
        self._test_get_transactions()

    def _get_transaction_service(self) -> GatewayTransactionService:
        return GatewayTransactionService(self.mock_node, 0)
