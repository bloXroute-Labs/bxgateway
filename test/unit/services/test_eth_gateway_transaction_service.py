from bxgateway.testing.abstract_eth_gateway_transaction_service_test import TestAbstractEthGatewayTransactionService


class EthGatewayTransactionServiceTest(TestAbstractEthGatewayTransactionService):

    def test_process_transactions_message_from_node(self):
        self._test_process_transactions_message_from_node()
