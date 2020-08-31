from bxgateway.services.extension_gateway_transaction_service import ExtensionGatewayTransactionService
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing.abstract_ont_gateway_transaction_service_test import TestAbstractOntGatewayTransactionService


class ExtensionOntGatewayTransactionServiceTest(TestAbstractOntGatewayTransactionService):

    def test_process_transactions_message_from_node(self):
        self._test_process_transactions_message_from_node("ontology")

    def _get_transaction_service(self) -> GatewayTransactionService:
        return ExtensionGatewayTransactionService(self.node, 0)
