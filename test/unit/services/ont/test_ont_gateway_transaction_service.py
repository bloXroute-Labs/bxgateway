from bxgateway.testing.abstract_ont_gateway_transaction_service_test import TestAbstractOntGatewayTransactionService


class OntGatewayTransactionServiceTest(TestAbstractOntGatewayTransactionService):

    def test_process_transactions_message_from_node(self):
        self._test_process_transactions_message_from_node()
