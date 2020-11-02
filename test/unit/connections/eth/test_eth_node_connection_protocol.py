import struct

from mock import MagicMock, call

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.models.tx_validation_status import TxValidationStatus
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.utils.blockchain_utils import transaction_validation
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxgateway.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxgateway.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.feed.new_transaction_feed import FeedSource
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import \
    NewBlockHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody
from bxgateway.testing import gateway_helpers
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
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
from bxgateway.testing.mocks import mock_eth_messages

NETWORK_NUM = 1


class EthNodeConnectionProtocolTest(AbstractTestCase):
    def setUp(self) -> None:
        pub_key = "a04f30a45aae413d0ca0f219b4dcb7049857bc3f91a6351288cce603a2c9646294a02b987bf6586b370b2c22d74662355677007a14238bb037aedf41c2d08866"
        node_ssl_service = MockNodeSSLService(EthGatewayNode.NODE_TYPE, MagicMock())
        opts = gateway_helpers.get_gateway_opts(
            8000,
            include_default_eth_args=True,
            blockchain_protocol=BlockchainProtocol.ETHEREUM.name,
            blockchain_network_num=5,
            pub_key=pub_key,
        )
        self.node = EthGatewayNode(opts, node_ssl_service)
        self.enqueued_messages = []
        self.broadcast_to_node_messages = []
        self.broadcast_messages = []
        self.node.opts.has_fully_updated_tx_service = True
        self.node.init_live_feeds()

        self.connection = helpers.create_connection(
            EthNodeConnection, self.node, opts, port=opts.blockchain_port
        )
        gateway_helpers.add_blockchain_peer(self.node, self.connection)
        self.block_queuing_service = self.node.block_queuing_service_manager.get_designated_block_queuing_service()

        self.connection.on_connection_established()
        self.node.node_conn = self.connection

        def mocked_enqueue_msg(msg, prepend=False):
            self.enqueued_messages.append(msg)

        self.connection.enqueue_msg = mocked_enqueue_msg

        def mocked_broadcast(msg, broadcasting_conn=None, prepend_to_queue=False, connection_types=None):
            if connection_types is None:
                connection_types = [ConnectionType.RELAY_ALL]
            if len(connection_types) == 1 and connection_types[0] == ConnectionType.BLOCKCHAIN_NODE:
                self.broadcast_to_node_messages.append(msg)
            else:
                self.broadcast_messages.append(msg)

        self.node.broadcast = mocked_broadcast

        self.cleanup_service = self.node.block_cleanup_service
        self.node.block_processing_service.queue_block_for_processing = MagicMock()

        self.sut = self.connection.connection_protocol
        self.sut._waiting_checkpoint_headers_request = False
        self.node.opts.ws = True
        self.node.publish_block = MagicMock()

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

        self.node.block_queuing_service_manager.push(block_hash, eth_block_info)
        self.enqueued_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(None, raw_hash, 1, 0, 0)
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.enqueued_messages))

        headers_sent = self.enqueued_messages[0]
        self.assertIsInstance(headers_sent, BlockHeadersEthProtocolMessage)
        self.assertEqual(1, len(headers_sent.get_block_headers()))
        self.assertEqual(
            block_hash, headers_sent.get_block_headers()[0].hash_object()
        )

        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash, eth_block_info
        )
        self.sut.msg_get_block_headers(message)
        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(2, len(self.enqueued_messages))

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

        self.node.block_queuing_service_manager.push(block_hash, eth_block_info)
        self.enqueued_messages.clear()

        self.sut.msg_proxy_request = MagicMock()

        block_number_bytes = struct.pack(">I", block_number)
        message = GetBlockHeadersEthProtocolMessage(
            None, block_number_bytes, 1, 0, 0
        )

        self.sut.msg_get_block_headers(message)
        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.enqueued_messages))

        headers_sent = self.enqueued_messages[0]
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
            self.node.block_queuing_service_manager.push(block_hash, block_message)
        self.block_queuing_service.best_sent_block = (1019, block_hashes[-1], 0)

        self.enqueued_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[10].binary, 10, 0, 0
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.enqueued_messages))

        headers_sent = self.enqueued_messages[0]
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
            self.node.block_queuing_service_manager.push(block_hash, block_message)

        # create fork
        block_message = InternalEthBlockInfo.from_new_block_msg(
            mock_eth_messages.new_block_eth_protocol_message(34, 1017)
        )
        block_hash = block_message.block_hash()
        block_hashes.append(block_hash)
        self.node.block_queuing_service_manager.push(block_hash, block_message)

        self.enqueued_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[-1].binary, 10, 0, 1
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_called_once()
        self.assertEqual(0, len(self.enqueued_messages))

    def test_msg_get_block_headers_missing_block(self):
        self.node.opts.max_block_interval_s = 0

        block_hashes = []
        for i in (j for j in range(20) if j != 17):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(i, i + 1000)
            )
            block_hash = block_message.block_hash()
            block_hashes.append(block_hash)
            self.node.block_queuing_service_manager.push(block_hash, block_message)

        self.enqueued_messages.clear()

        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[-1].binary, 10, 0, 1
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_called_once()
        self.assertEqual(0, len(self.enqueued_messages))

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
            self.node.block_queuing_service_manager.push(block_hash, block_message)

        self.enqueued_messages.clear()
        self.sut.msg_proxy_request = MagicMock()

        block_height_bytes = block_heights[-1] + 4
        block_height_bytes = block_height_bytes.to_bytes(8, "big")

        message = GetBlockHeadersEthProtocolMessage(
            None, block_height_bytes, 1, 0, 0
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.enqueued_messages))
        headers_sent = self.enqueued_messages[0]
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
            self.node.block_queuing_service_manager.push(
                block_hashes[i], block_messages[i]
            )
            self.block_queuing_service.remove_from_queue(block_hashes[i])
        self.block_queuing_service.best_sent_block = (1019, block_hashes[-1], 0)

        self.enqueued_messages.clear()
        self.sut.msg_proxy_request = MagicMock()
        message = GetBlockHeadersEthProtocolMessage(
            None, block_hashes[10].binary, 10, 0, 0
        )
        self.sut.msg_get_block_headers(message)

        self.sut.msg_proxy_request.assert_called_once()
        self.sut.msg_proxy_request.reset_mock()

        self.sut.msg_block(block_messages[17].to_new_block_msg())
        self.node.publish_block.assert_called()

        self.sut.msg_get_block_headers(message)
        self.sut.msg_proxy_request.assert_not_called()
        self.assertEqual(1, len(self.enqueued_messages))

        headers_sent = self.enqueued_messages[0]
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
        self.assertEqual(1, len(self.broadcast_messages))
        self.assertEqual(1, len(self.broadcast_to_node_messages))

    def test_handle_tx_with_an_invalid_signature(self):
        tx_bytes = \
            b"\xf8k" \
            b"!" \
            b"\x85\x0b\xdf\xd6>\x00" \
            b"\x82R\x08\x94" \
            b"\xf8\x04O\xf8$\xc2\xdc\xe1t\xb4\xee\x9f\x95\x8c*s\x84\x83\x18\x9e" \
            b"\x87\t<\xaf\xacj\x80\x00\x80" \
            b"!" \
            b"\xa0-\xbf,\xa9+\xae\xabJ\x03\xcd\xfa\xe3<\xbf$\x00e\xe2N|\xc9\xf7\xe2\xa9\x9c>\xdfn\x0cO\xc0\x16" \
            b"\xa0)\x11K=;\x96X}a\xd5\x00\x06eSz\xd1,\xe4>\xa1\x8c\xf8\x7f>\x0e:\xd1\xcd\x00?'?"

        result = transaction_validation.validate_transaction(
            tx_bytes,
            BlockchainProtocol(self.node.network.protocol.lower()),
            self.node.get_network_min_transaction_fee()
        )

        self.assertEqual(TxValidationStatus.INVALID_SIGNATURE, result)

    def test_handle_tx_with_an_invalid_format(self):
        tx_bytes = \
            b"\xf8k" \
            b"!" \
            b"\x85\x0b\xdf\xd6>\x00\x82R\x08\x94\xf8\x04O\xf8$\xc2\xdc\xe1t\xb4\xee\x9f\x95\x8c*s\x84\x83" \
            b"\x18\x9e\x87\t<\xaf\xacj\x80\x00\x80&\xa0-\xbf,\xa9+\xae\xabJ\x03\xcd\xfa\xe3<\xbf$\x00e\xe2N|\xc9" \
            b"\xf7\xe2\xa9\x9c>\xdfn\x0cO\xc0\x16"

        result = transaction_validation.validate_transaction(
            tx_bytes,
            BlockchainProtocol(self.node.network.protocol.lower()),
            self.node.get_network_min_transaction_fee()
        )

        self.assertEqual(TxValidationStatus.INVALID_FORMAT, result)

    def test_handle_tx_with_low_fee(self):
        self.node.feed_manager.publish_to_feed = MagicMock()
        self.node.opts.ws = False

        tx_bytes = \
            b"\xf8k" \
            b"!" \
            b"\x85\x0b\xdf\xd6>\x00\x82R\x08\x94\xf8\x04O\xf8$\xc2\xdc\xe1t\xb4\xee\x9f\x95\x8c*s\x84\x83" \
            b"\x18\x9e\x87\t<\xaf\xacj\x80\x00\x80" \
            b"&" \
            b"\xa0" \
            b"-\xbf,\xa9+\xae\xabJ\x03\xcd\xfa\xe3<\xbf$\x00e\xe2N|\xc9\xf7\xe2\xa9\x9c>\xdfn\x0cO\xc0\x16" \
            b"\xa0)\x11K=;\x96X}a\xd5\x00\x06eSz\xd1,\xe4>\xa1\x8c\xf8\x7f>\x0e:\xd1\xcd\x00?'\x15"

        result = transaction_validation.validate_transaction(
            tx_bytes,
            BlockchainProtocol(self.node.network.protocol.lower()),
            510000000000
        )
        self.assertEqual(TxValidationStatus.LOW_FEE, result)

    def test_complete_header_body_fetch(self):
        self.node.block_processing_service.queue_block_for_processing = MagicMock()
        self.sut.is_valid_block_timestamp = MagicMock(return_value=True)

        header = mock_eth_messages.get_dummy_block_header(1)
        block = mock_eth_messages.get_dummy_block(1, header)
        block_hash = header.hash_object()
        new_block_hashes_message = NewBlockHashesEthProtocolMessage.from_block_hash_number_pair(
            block_hash, 1
        )
        header_message = BlockHeadersEthProtocolMessage(None, [header])
        bodies_message = BlockBodiesEthProtocolMessage(None, [TransientBlockBody(block.transactions, block.uncles)])

        self.sut.msg_new_block_hashes(new_block_hashes_message)
        self.assertEqual(1, len(self.sut.pending_new_block_parts.contents))
        self.assertEqual(2, len(self.enqueued_messages))

        self.sut.msg_block_headers(header_message)
        self.sut.msg_block_bodies(bodies_message)

        self.node.block_processing_service.queue_block_for_processing.assert_called_once()

    def test_header_body_fetch_abort_from_bdn(self):
        self.node.block_processing_service.queue_block_for_processing = MagicMock()
        self.sut.is_valid_block_timestamp = MagicMock(return_value=True)

        header = mock_eth_messages.get_dummy_block_header(1)
        block = mock_eth_messages.get_dummy_block(1, header)
        block_hash = header.hash_object()
        new_block_hashes_message = NewBlockHashesEthProtocolMessage.from_block_hash_number_pair(
            block_hash, 1
        )
        header_message = BlockHeadersEthProtocolMessage(None, [header])
        bodies_message = BlockBodiesEthProtocolMessage(None, [TransientBlockBody(block.transactions, block.uncles)])
        internal_block_info = InternalEthBlockInfo.from_new_block_msg(NewBlockEthProtocolMessage(None, block, 1))

        self.sut.msg_new_block_hashes(new_block_hashes_message)
        self.assertEqual(1, len(self.sut.pending_new_block_parts.contents))
        self.assertEqual(2, len(self.enqueued_messages))

        self.node.on_block_received_from_bdn(block_hash, internal_block_info)

        self.sut.msg_block_headers(header_message)
        self.sut.msg_block_bodies(bodies_message)

        self.node.block_processing_service.queue_block_for_processing.assert_not_called()
