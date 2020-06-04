from typing import Union, Tuple, List

from mock import MagicMock

from bxcommon.test_utils import helpers
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.services.extension_gateway_transaction_service import ExtensionGatewayTransactionService
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing.abstract_eth_gateway_transaction_service_test import TestAbstractEthGatewayTransactionService
from bxgateway.testing.abstract_gateway_transaction_service_test import TestAbstractGatewayTransactionService
from bxgateway.testing.mocks import mock_eth_messages


class ExtensionEthGatewayTransactionServiceTest(TestAbstractEthGatewayTransactionService):

    def test_process_transactions_message_from_node(self):
        self._test_process_transactions_message_from_node()

    def _get_transaction_service(self) -> GatewayTransactionService:
        return ExtensionGatewayTransactionService(self.node, 0)