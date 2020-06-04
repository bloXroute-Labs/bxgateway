from typing import Union, Tuple

from mock import MagicMock

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing.abstract_gateway_transaction_service_test import TestAbstractGatewayTransactionService
from bxgateway.testing.mocks import mock_eth_messages


class TestAbstractEthGatewayTransactionService(TestAbstractGatewayTransactionService):

    def _get_transaction_service(self) -> GatewayTransactionService:
        return GatewayTransactionService(self.node, 0)

    def _get_gateway_node(self) -> AbstractGatewayNode:
        mockSslService = MagicMock()
        return EthGatewayNode(self.opts, mockSslService)

    def _get_node_tx_message(
        self
    ) -> Tuple[Union[TxOntMessage, TransactionsEthProtocolMessage], Sha256Hash, Union[bytearray, memoryview]]:
        txs = [
            mock_eth_messages.get_dummy_transaction(1)
        ]
        msg = TransactionsEthProtocolMessage(None, txs)
        raw_bytes = msg.rawbytes()

        another_msg = TransactionsEthProtocolMessage(raw_bytes)
        deserialized_txs = another_msg.get_transactions()
        self.assertEqual(1, len(deserialized_txs))

        return msg, msg.get_transactions()[0].hash(), msg.get_transactions()[0].contents()
