from mock import MagicMock
from typing import Tuple, Union

from bxcommon.test_utils import helpers
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing.abstract_gateway_transaction_service_test import TestAbstractGatewayTransactionService


class TestAbstractOntGatewayTransactionService(TestAbstractGatewayTransactionService):

    def _get_transaction_service(self) -> GatewayTransactionService:
        return GatewayTransactionService(self.node, 0)

    def _get_gateway_node(self) -> AbstractGatewayNode:
        mockSslService = MagicMock()
        return OntGatewayNode(self.opts, mockSslService)

    def _get_node_tx_message(
        self
    ) -> Tuple[Union[TxOntMessage, TransactionsEthProtocolMessage], Sha256Hash, Union[bytearray, memoryview]]:
        magic = 123456
        version = 1
        tx_contents = helpers.generate_bytearray(200)
        msg = TxOntMessage(magic, version, tx_contents)
        return msg, msg.tx_hash(), msg.payload()