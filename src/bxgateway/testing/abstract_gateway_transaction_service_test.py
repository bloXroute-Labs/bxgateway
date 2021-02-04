from abc import ABCMeta, abstractmethod
from typing import Union, Tuple, List

from bxcommon import constants
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_transaction_service_test_case import AbstractTransactionServiceTestCase
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.testing import gateway_helpers


class TestAbstractGatewayTransactionService(AbstractTransactionServiceTestCase, metaclass=ABCMeta):

    def setUp(self) -> None:
        pub_key = "a04f30a45aae413d0ca0f219b4dcb7049857bc3f91a6351288cce603a2c9646294a02b987bf6586b370b2c22d74662355677007a14238bb037aedf41c2d08866"

        self.opts = gateway_helpers.get_gateway_opts(
            8000,
            include_default_eth_args=True,
            pub_key=pub_key,
            blockchain_network_num=5,
            account_id="12345"
        )

        if self.opts.use_extensions:
            helpers.set_extensions_parallelism()
        self.node = self._get_gateway_node()
        self.transaction_service = self._get_transaction_service()

    def _test_process_transactions_message_from_node(self):
        min_tx_network_fee = 0
        self.node.account_id = "12345"
        if self.node.network_num in self.node.opts.blockchain_networks:
            min_tx_network_fee = self.node.opts.blockchain_networks[self.node.network_num].min_tx_network_fee

        tx_message, test_txs_info = self._get_node_tx_message()
        self.opts.transaction_validation = False
        result = self.transaction_service.process_transactions_message_from_node(
            tx_message, min_tx_network_fee, True
        )

        self.assertIsNotNone(result)
        self.assertEqual(len(test_txs_info), len(result))

        total_size = 0

        for (test_tx_hash, test_tx_contents), tx_result in zip(test_txs_info, result):
            transaction_key = self.transaction_service.get_transaction_key(test_tx_hash)
            self.assertFalse(tx_result.seen)
            self.assertEqual(convert.bytes_to_hex(test_tx_hash.binary),
                             convert.bytes_to_hex(tx_result.transaction_hash.binary))
            tx_contents = tx_result.transaction_contents
            self.assertEqual(convert.bytes_to_hex(test_tx_contents), convert.bytes_to_hex(tx_contents))

            self.assertTrue(self.transaction_service.has_transaction_contents(test_tx_hash))
            self.assertEqual(convert.bytes_to_hex(test_tx_contents),
                             convert.bytes_to_hex(self.transaction_service.get_transaction_by_key(transaction_key)))

            total_size += len(test_tx_contents)

        self.assertEqual(0, len(self.transaction_service._tx_assignment_expire_queue.queue))
        self.assertEqual(len(result), len(self.transaction_service.tx_hashes_without_short_id))
        self.assertEqual(0, len(self.transaction_service.tx_hashes_without_content))
        self.assertEqual(total_size, self.transaction_service._total_tx_contents_size)


    @abstractmethod
    def _get_transaction_service(self) -> GatewayTransactionService:
        pass

    @abstractmethod
    def _get_gateway_node(self) -> AbstractGatewayNode:
        pass

    @abstractmethod
    def _get_node_tx_message(
        self
    ) -> Tuple[Union[TxOntMessage, TransactionsEthProtocolMessage], List[Tuple[Sha256Hash, Union[bytearray, memoryview]]]]:
        pass
