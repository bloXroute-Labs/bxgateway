import struct

from mock import MagicMock, call

from bxgateway.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxgateway.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.new_transaction_feed import FeedSource
from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxgateway.testing import gateway_helpers
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxgateway.connections.abstract_gateway_blockchain_connection import (
    AbstractGatewayBlockchainConnection,
)
from bxgateway.connections.eth.eth_node_connection_protocol import (
    EthNodeConnectionProtocol,
)
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import (
    BlockBodiesEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import (
    BlockHeadersEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import (
    GetBlockHeadersEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import (
    NewBlockEthProtocolMessage,
)
from bxgateway.services.eth.eth_block_processing_service import (
    EthBlockProcessingService,
)
from bxgateway.services.eth.eth_block_queuing_service import (
    EthBlockQueuingService,
)
from bxgateway.services.eth.eth_normal_block_cleanup_service import (
    EthNormalBlockCleanupService,
)
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode

NETWORK_NUM = 1


class EthNodeConnectionProtocolTest(AbstractTestCase):
    def setUp(self) -> None:

        opts = gateway_helpers.get_gateway_opts(8000, include_default_eth_args=True)

        self.node = MockGatewayNode(opts)

        self.connection = MagicMock(spec=AbstractGatewayBlockchainConnection)
        self.connection.node = self.node
        self.connection.is_active = MagicMock(return_value=True)
        self.connection.network_num = 5
        self.connection.format_connection_desc = "127.0.0.1:12345 - B"
        self.node.set_known_total_difficulty = MagicMock()
        self.node.node_conn = self.connection
        self.node.message_converter = EthNormalMessageConverter()

        self.cleanup_service = EthNormalBlockCleanupService(
            self.node, NETWORK_NUM
        )
        self.node.block_cleanup_service = self.cleanup_service
        self.node.block_processing_service = EthBlockProcessingService(
            self.node
        )
        self.node.block_processing_service.queue_block_for_processing = MagicMock()
        self.node.block_queuing_service = EthBlockQueuingService(self.node)

        dummy_private_key = crypto_utils.make_private_key(
            helpers.generate_bytearray(111)
        )
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)
        self.sut = EthNodeConnectionProtocol(
            self.connection, True, dummy_private_key, dummy_public_key
        )
        self.sut._waiting_checkpoint_headers_request = False

    def test_request_block_bodies(self):
        self.cleanup_service.clean_block_transactions_by_block_components = (
            MagicMock()
        )

        block_hashes_sets = [
            [helpers.generate_object_hash(), helpers.generate_object_hash()],
            [helpers.generate_object_hash()],
            [helpers.generate_object_hash()],
        ]

        block_bodies_messages = [
            BlockBodiesEthProtocolMessage(
                None,
                [
                    mock_eth_messages.get_dummy_transient_block_body(1),
                    mock_eth_messages.get_dummy_transient_block_body(2),
                ],
            ),
            BlockBodiesEthProtocolMessage(
                None, [mock_eth_messages.get_dummy_transient_block_body(3)]
            ),
            BlockBodiesEthProtocolMessage(
                None, [mock_eth_messages.get_dummy_transient_block_body(4)]
            ),
        ]

        transactions = [
            [
                [
                    tx.hash()
                    for tx in BlockBodiesEthProtocolMessage.from_body_bytes(
                        block_body_bytes
                    )
                    .get_blocks()[0]
                    .transactions
                ]
                for block_body_bytes in msg.get_block_bodies_bytes()
            ]
            for msg in block_bodies_messages
        ]

        for block_bodies_message in block_bodies_messages:
            block_bodies_message.serialize()

        for block_hashes_set in block_hashes_sets:
            for block_hash in block_hashes_set:
                self.node.block_cleanup_service._block_hash_marked_for_cleanup.add(
                    block_hash
                )
            self.sut.request_block_body(block_hashes_set)

        self.sut.msg_block_bodies(block_bodies_messages[0])
        call_args_list = (
            self.cleanup_service.clean_block_transactions_by_block_components.call_args_list
        )

        first_call = call_args_list[0]
        first_kwargs = first_call[1]
        self.assertEqual(
            first_kwargs["transaction_service"], self.node.get_tx_service()
        )
        self.assertEqual(first_kwargs["block_hash"], block_hashes_sets[0][0])
        self.assertEqual(
            list(first_kwargs["transactions_list"]), transactions[0][0]
        )

        second_call = call_args_list[1]
        second_kwargs = second_call[1]
        self.assertEqual(
            second_kwargs["transaction_service"], self.node.get_tx_service()
        )
        self.assertEqual(second_kwargs["block_hash"], block_hashes_sets[0][1])
        self.assertEqual(
            list(second_kwargs["transactions_list"]), transactions[0][1]
        )

        self.cleanup_service.clean_block_transactions_by_block_components.reset_mock()
        self.sut.msg_block_bodies(block_bodies_messages[1])
        call_args_list = (
            self.cleanup_service.clean_block_transactions_by_block_components.call_args_list
        )

        first_call = call_args_list[0]
        first_kwargs = first_call[1]
        self.assertEqual(
            first_kwargs["transaction_service"], self.node.get_tx_service()
        )
        self.assertEqual(first_kwargs["block_hash"], block_hashes_sets[1][0])
        self.assertEqual(
            list(first_kwargs["transactions_list"]), transactions[1][0]
        )

        self.cleanup_service.clean_block_transactions_by_block_components.reset_mock()
        self.sut.msg_block_bodies(block_bodies_messages[2])
        call_args_list = (
            self.cleanup_service.clean_block_transactions_by_block_components.call_args_list
        )

        first_call = call_args_list[0]
        first_kwargs = first_call[1]
        self.assertEqual(
            first_kwargs["transaction_service"], self.node.get_tx_service()
        )
        self.assertEqual(first_kwargs["block_hash"], block_hashes_sets[2][0])
        self.assertEqual(
            list(first_kwargs["transactions_list"]), transactions[2][0]
        )

    def test_msg_get_block_headers_unknown(self):
        block_hash = helpers.generate_hash()
        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(None, block_hash, 1, 0, 0)

        self.sut.msg_get_block_headers(message)
        self.sut.msg_proxy_request.assert_called_once()

    def test_msg_get_block_headers_known_single(self):
        header = mock_eth_messages.get_dummy_block_header(12)
        block = mock_eth_messages.get_dummy_block(1, header)
        raw_hash = header.hash()
        block_hash = header.hash_object()
        new_block_message = NewBlockEthProtocolMessage(None, block, 10)
        eth_block_info = InternalEthBlockInfo.from_new_block_msg(
            new_block_message
        )

        self.node.block_queuing_service.push(block_hash, eth_block_info)
        self.node.send_to_node_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(None, raw_hash, 1, 0, 0)
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.node.send_to_node_messages))

        headers_sent = self.node.send_to_node_messages[0]
        self.assertIsInstance(headers_sent, BlockHeadersEthProtocolMessage)
        self.assertEqual(1, len(headers_sent.get_block_headers()))
        self.assertEqual(
            block_hash, headers_sent.get_block_headers()[0].hash_object()
        )

        self.node.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash, eth_block_info
        )
        self.sut.msg_get_block_headers(message)
        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(2, len(self.node.send_to_node_messages))

    def test_msg_get_block_headers_block_number(self):
        block_number = 123456
        header = mock_eth_messages.get_dummy_block_header(
            12, block_number=block_number
        )
        block = mock_eth_messages.get_dummy_block(1, header)
        block_hash = header.hash_object()
        new_block_message = NewBlockEthProtocolMessage(None, block, 10)
        eth_block_info = InternalEthBlockInfo.from_new_block_msg(
            new_block_message
        )

        self.node.block_queuing_service.push(block_hash, eth_block_info)
        self.node.send_to_node_messages.clear()

        self.sut.msg_proxy_request = MagicMock()

        block_number_bytes = struct.pack(">I", block_number)
        message = GetBlockHeadersEthProtocolMessage(
            None, block_number_bytes, 1, 0, 0
        )

        self.sut.msg_get_block_headers(message)
        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.node.send_to_node_messages))

        headers_sent = self.node.send_to_node_messages[0]
        self.assertIsInstance(headers_sent, BlockHeadersEthProtocolMessage)
        self.assertEqual(1, len(headers_sent.get_block_headers()))
        self.assertEqual(
            block_hash, headers_sent.get_block_headers()[0].hash_object()
        )

    def test_msg_get_block_headers_known_many(self):
        self.node.opts.max_block_interval_s = 0

        block_hashes = []
        block_hash = None
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(
                    i, i + 1000, block_hash
                )
            )
            block_hash = block_message.block_hash()
            block_hashes.append(block_hash)
            self.node.block_queuing_service.push(block_hash, block_message)
        self.node.block_queuing_service.best_sent_block = (1019, block_hashes[-1], 0)

        self.node.send_to_node_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[10].binary, 10, 0, 0
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.node.send_to_node_messages))

        headers_sent = self.node.send_to_node_messages[0]
        self.assertIsInstance(headers_sent, BlockHeadersEthProtocolMessage)
        self.assertEqual(10, len(headers_sent.get_block_headers()))

        for i in range(10):
            self.assertEqual(
                block_hashes[i + 10],
                headers_sent.get_block_headers()[i].hash_object(),
            )

    def test_msg_get_block_headers_fork(self):
        self.node.opts.max_block_interval_s = 0

        block_hashes = []
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000)
            )
            block_hash = block_message.block_hash()
            block_hashes.append(block_hash)
            self.node.block_queuing_service.push(block_hash, block_message)

        # create fork
        block_message = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(34, 1017)
        )
        block_hash = block_message.block_hash()
        block_hashes.append(block_hash)
        self.node.block_queuing_service.push(block_hash, block_message)

        self.node.send_to_node_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[-1].binary, 10, 0, 1
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_called_once()
        self.assertEqual(0, len(self.node.send_to_node_messages))

    def test_msg_get_block_headers_missing_block(self):
        self.node.opts.max_block_interval_s = 0

        block_hashes = []
        for i in (j for j in range(20) if j != 17):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000)
            )
            block_hash = block_message.block_hash()
            block_hashes.append(block_hash)
            self.node.block_queuing_service.push(block_hash, block_message)

        self.node.send_to_node_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[-1].binary, 10, 0, 1
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_called_once()
        self.assertEqual(0, len(self.node.send_to_node_messages))

    def test_msg_get_block_headers_future_block(self):
        self.node.opts.max_block_interval_s = 0

        block_hashes = []
        block_heights = []
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000)
            )
            block_hash = block_message.block_hash()
            block_heights.append(block_message.block_number())
            block_hashes.append(block_hash)
            self.node.block_queuing_service.push(block_hash, block_message)

        self.node.send_to_node_messages.clear()
        self.sut.msg_proxy_request = MagicMock()

        block_height_bytes = block_heights[-1] + 4
        block_height_bytes = block_height_bytes.to_bytes(8, "big")

        message = GetBlockHeadersEthProtocolMessage(
            None, block_height_bytes, 1, 0, 0
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.node.send_to_node_messages))
        headers_sent = self.node.send_to_node_messages[0]
        self.assertIsInstance(headers_sent, BlockHeadersEthProtocolMessage)
        self.assertEqual(0, len(headers_sent.get_block_headers()))

    def test_msg_block_adds_headers_and_bodies(self):
        self.node.opts.max_block_interval_s = 0
        self.sut.is_valid_block_timestamp = MagicMock(return_value=True)

        block_hashes = []
        block_messages = []
        block_hash = None
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000, block_hash)
            )
            block_hash = block_message.block_hash()
            block_hashes.append(block_hash)
            block_messages.append(block_message)

        # push all blocks, except for # 17
        for i in (j for j in range(20) if j != 17):
            self.node.block_queuing_service.push(
                block_hashes[i], block_messages[i]
            )
            self.node.block_queuing_service.remove_from_queue(block_hashes[i])
        self.node.block_queuing_service.best_sent_block = (1019, block_hashes[-1], 0)

        self.node.send_to_node_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[10].binary, 10, 0, 0
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_called_once()
        self.assertEqual(0, len(self.node.send_to_node_messages))
        self.sut.msg_proxy_request.reset_mock()

        self.sut.msg_block(block_messages[17].to_new_block_msg())
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.node.send_to_node_messages))

        headers_sent = self.node.send_to_node_messages[0]
        self.assertIsInstance(headers_sent, BlockHeadersEthProtocolMessage)
        self.assertEqual(10, len(headers_sent.get_block_headers()))

        for i in range(10):
            self.assertEqual(
                block_hashes[i + 10],
                headers_sent.get_block_headers()[i].hash_object(),
            )

    def test_msg_tx(self):
        self.node.feed_manager.publish_to_feed = MagicMock()
        self.node.opts.ws = True

        transaction = mock_eth_messages.get_dummy_transaction(1)
        transaction_hash = transaction.hash()
        transaction_contents = transaction.contents()
        messages = TransactionsEthProtocolMessage(None, [transaction])

        self.sut.msg_tx(messages)

        # published to both feeds
        self.node.feed_manager.publish_to_feed.assert_has_calls(
            [
                call(
                    EthNewTransactionFeed.NAME,
                    EthRawTransaction(transaction_hash, transaction_contents, FeedSource.BLOCKCHAIN_SOCKET)
                ),
                call(
                    EthPendingTransactionFeed.NAME,
                    EthRawTransaction(transaction_hash, transaction_contents, FeedSource.BLOCKCHAIN_SOCKET)
                ),
            ]
        )

        # broadcast transactions
        self.assertEqual(1, len(self.node.broadcast_messages))
