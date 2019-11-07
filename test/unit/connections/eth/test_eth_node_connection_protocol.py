from mock import MagicMock, call

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.services.eth.eth_normal_block_cleanup_service import EthNormalBlockCleanupService
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.eth import crypto_utils

NETWORK_NUM = 1


class EthNodeConnectionProtocolTest(AbstractTestCase):

    def setUp(self) -> None:

        opts = helpers.get_gateway_opts(8000, include_default_eth_args=True)
        self.node = MockGatewayNode(opts)

        self.connection = MagicMock(spec=AbstractGatewayBlockchainConnection)
        self.connection.node = self.node
        self.cleanup_service = EthNormalBlockCleanupService(self.node, NETWORK_NUM)
        self.node.block_cleanup_service = self.cleanup_service

        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)
        self.sut = EthNodeConnectionProtocol(self.connection, True, dummy_private_key, dummy_public_key)

    def test_request_block_bodies(self):
        self.cleanup_service.clean_block_transactions_by_block_components = MagicMock()

        block_hashes_sets = [
            [helpers.generate_object_hash(), helpers.generate_object_hash()],
            [helpers.generate_object_hash()],
            [helpers.generate_object_hash()]
        ]

        block_bodies_messages = [
            BlockBodiesEthProtocolMessage(None, [
                mock_eth_messages.get_dummy_transient_block_body(1),
                mock_eth_messages.get_dummy_transient_block_body(2)
            ]),
            BlockBodiesEthProtocolMessage(None, [mock_eth_messages.get_dummy_transient_block_body(3)]),
            BlockBodiesEthProtocolMessage(None, [mock_eth_messages.get_dummy_transient_block_body(4)])
        ]

        transactions = [
            [
                [
                    tx.hash()
                    for tx in
                    BlockBodiesEthProtocolMessage.from_body_bytes(block_body_bytes).get_blocks()[0].transactions
                ]
                for block_body_bytes in
                msg.get_block_bodies_bytes()
            ]
            for msg in block_bodies_messages
        ]

        for block_bodies_message in block_bodies_messages:
            block_bodies_message.serialize()

        for block_hashes_set in block_hashes_sets:
            for block_hash in block_hashes_set:
                self.node.block_cleanup_service._block_hash_marked_for_cleanup.add(block_hash)
            self.sut.request_block_body(block_hashes_set)

        self.sut.msg_block_bodies(block_bodies_messages[0])
        call_args_list = self.cleanup_service.clean_block_transactions_by_block_components.call_args_list

        first_call = call_args_list[0]
        first_kwargs = first_call[1]
        self.assertEqual(first_kwargs["transaction_service"], self.node.get_tx_service())
        self.assertEqual(first_kwargs["block_hash"], block_hashes_sets[0][0])
        self.assertEqual(list(first_kwargs["transactions_list"]), transactions[0][0])

        second_call = call_args_list[1]
        second_kwargs = second_call[1]
        self.assertEqual(second_kwargs["transaction_service"], self.node.get_tx_service())
        self.assertEqual(second_kwargs["block_hash"], block_hashes_sets[0][1])
        self.assertEqual(list(second_kwargs["transactions_list"]), transactions[0][1])

        self.cleanup_service.clean_block_transactions_by_block_components.reset_mock()
        self.sut.msg_block_bodies(block_bodies_messages[1])
        call_args_list = self.cleanup_service.clean_block_transactions_by_block_components.call_args_list

        first_call = call_args_list[0]
        first_kwargs = first_call[1]
        self.assertEqual(first_kwargs["transaction_service"], self.node.get_tx_service())
        self.assertEqual(first_kwargs["block_hash"], block_hashes_sets[1][0])
        self.assertEqual(list(first_kwargs["transactions_list"]), transactions[1][0])

        self.cleanup_service.clean_block_transactions_by_block_components.reset_mock()
        self.sut.msg_block_bodies(block_bodies_messages[2])
        call_args_list = self.cleanup_service.clean_block_transactions_by_block_components.call_args_list

        first_call = call_args_list[0]
        first_kwargs = first_call[1]
        self.assertEqual(first_kwargs["transaction_service"], self.node.get_tx_service())
        self.assertEqual(first_kwargs["block_hash"], block_hashes_sets[2][0])
        self.assertEqual(list(first_kwargs["transactions_list"]), transactions[2][0])
