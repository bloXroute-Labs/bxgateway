from abc import ABCMeta, abstractmethod
from unittest.mock import MagicMock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_transaction_service_test_case import AbstractTransactionServiceTestCase
from bxcommon.utils import convert
from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing import gateway_helpers


class TestAbstractGatewayTransactionService(AbstractTransactionServiceTestCase, metaclass=ABCMeta):

    def setUp(self) -> None:
        opts = gateway_helpers.get_gateway_opts(8000, include_default_ont_args=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        mockSslService = MagicMock()
        self.node = OntGatewayNode(opts, mockSslService)
        self.transaction_service = self._get_transaction_service()

    def _test_process_transactions_message_from_node(self):
        magic = 123456
        version = 1
        tx_contents = helpers.generate_bytearray(200)

        ont_tx_message = TxOntMessage(magic, version, tx_contents)
        result = self.transaction_service.process_transactions_message_from_node(ont_tx_message)

        self.assertIsNotNone(result)
        self.assertEqual(1, len(result))

        first_item = result[0]

        self.assertFalse(first_item.seen)
        self.assertEqual(convert.bytes_to_hex(ont_tx_message.tx_hash().binary),
                         convert.bytes_to_hex(first_item.transaction_hash.binary))
        tx_contents = first_item.transaction_contents
        self.assertEqual(convert.bytes_to_hex(ont_tx_message.payload()), convert.bytes_to_hex(tx_contents))

    @abstractmethod
    def _get_transaction_service(self) -> GatewayTransactionService:
        pass
