from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing.abstract_gateway_transaction_service_test import TestAbstractGatewayTransactionService


class GatewayTransactionServiceTest(TestAbstractGatewayTransactionService):

    def test_process_transactions_message_from_node(self):
        self._test_process_transactions_message_from_node()

    def _get_transaction_service(self) -> GatewayTransactionService:
        return GatewayTransactionService(self.node, 0)
