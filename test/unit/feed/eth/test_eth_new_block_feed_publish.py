import time
from unittest.mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxcommon.test_utils import helpers
from bxcommon.feed.feed_source import FeedSource

from bxgateway.feed.eth.eth_new_block_feed import EthNewBlockFeed
from bxgateway.feed.eth.eth_raw_block import EthRawBlock

from bxgateway.gateway_constants import LOCALHOST
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

_recover_public_key = crypto_utils.recover_public_key


class EthNewBlockFeedPublishTest(AbstractTestCase):

    def setUp(self) -> None:

        crypto_utils.recover_public_key = MagicMock(
            return_value=bytes(32))

        pub_key = "a04f30a45aae413d0ca0f219b4dcb7049857bc3f91a6351288cce603a2c9646294a02b987bf6586b370b2c22d74662355677007a14238bb037aedf41c2d08866"
        opts = gateway_helpers.get_gateway_opts(8000, include_default_eth_args=True, pub_key=pub_key)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        node_ssl_service = MockNodeSSLService(EthGatewayNode.NODE_TYPE, MagicMock())
        self.node = EthGatewayNode(opts, node_ssl_service)
        self.node_conn = MockConnection(
            MockSocketConnection(1, self.node, ip_address=LOCALHOST, port=8002), self.node
            )
        self.node.connection_pool.add(1, LOCALHOST, 8002, self.node_conn)
        gateway_helpers.add_blockchain_peer(self.node, self.node_conn)
        self.block_queuing_service = self.node.block_queuing_service_manager.get_designated_block_queuing_service()

        self.sut = EthNewBlockFeed(self.node)

    def generate_new_eth_block(self, block_number=10) -> InternalEthBlockInfo:
        block_message = NewBlockEthProtocolMessage(
            None,
            mock_eth_messages.get_dummy_block(
                1,
                mock_eth_messages.get_dummy_block_header(
                    5,
                    int(time.time()),
                    block_number=block_number
                )
            ),
            10
        )
        block_message.serialize()
        internal_block_message = InternalEthBlockInfo.from_new_block_msg(block_message)
        return internal_block_message

    def generate_raw_block_msg(self, block_number=10, source=FeedSource.BLOCKCHAIN_SOCKET):
        block_msg = self.generate_new_eth_block(block_number)
        return EthRawBlock(
            block_msg.block_number(), block_msg.block_hash(), source, self.node._get_block_message_lazy(block_msg, None)
        )

    def tearDown(self) -> None:
        crypto_utils.recover_public_key = _recover_public_key

    def test_lazy_get_block(self):
        block_msg = self.generate_new_eth_block()
        block_hash = block_msg.block_hash()

        # raises when block is absent from block queueing service
        lazy_block = self.node._get_block_message_lazy(None, block_hash)
        with self.assertRaises(StopIteration):
            block = next(lazy_block)
            self.assertIsNone(block)

        # returns block
        self.node.block_parts_storage[block_hash] = block_msg.to_new_block_parts()
        lazy_block = self.node._get_block_message_lazy(None, block_hash)
        block = next(lazy_block)
        self.assertEqual(block.block_hash(), block_hash)

        # returns block
        lazy_block = self.node._get_block_message_lazy(block_msg, None)
        next(lazy_block)
        self.assertEqual(block.block_hash(), block_hash)

    def test_block_seen_from_node(self):
        self.node.publish_block = MagicMock()
        block_msg = self.generate_new_eth_block()
        block_hash = block_msg.block_hash()
        block_number = block_msg.block_number()

        # called without block_msg
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash,
            None,
            block_number,
        )
        # called once with block
        self.node.publish_block.assert_not_called()

        # called for same block with message
        self.node.publish_block = MagicMock()
        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            block_hash,
            block_msg,
            block_number,
        )
        self.node.publish_block.assert_called_with(
            block_number, block_hash, block_msg, FeedSource.BLOCKCHAIN_SOCKET
        )

        # call for stale block
        stale_block_msg = self.generate_new_eth_block(9)
        stale_block_hash = stale_block_msg.block_hash()
        stale_block_number = stale_block_msg.block_number()
        self.node.publish_block = MagicMock()

        self.block_queuing_service.mark_block_seen_by_blockchain_node(
            stale_block_hash,
            stale_block_msg,
            stale_block_number,
        )
        self.node.publish_block.assert_not_called()

    def test_push_block(self):
        self.node.publish_block = MagicMock()
        block_msg = self.generate_new_eth_block()
        block_hash = block_msg.block_hash()
        block_number = block_msg.block_number()

        self.node.block_queuing_service_manager.push(
            block_hash, block_msg
        )
        self.node.publish_block.assert_called_once()
        self.node.publish_block.assert_called_with(
            block_number, block_hash, block_msg, FeedSource.BDN_SOCKET
        )

    def test_publish_fork(self):
        # in case of a fork publish current block,
        # and attempt to publish all following blocks from the queueing service

        self.sut.subscriber_count = MagicMock(return_value=10)
        self.sut.publish_blocks_from_queue = MagicMock()

        self.sut.publish(self.generate_raw_block_msg(10))
        self.sut.publish(self.generate_raw_block_msg(11))
        self.sut.publish(self.generate_raw_block_msg(12))


        fork_msg = self.generate_raw_block_msg(10)
        self.sut.publish(fork_msg)

        self.assertIn(fork_msg.block_hash, self.sut.published_blocks.contents)
        self.sut.publish_blocks_from_queue.assert_called_with(
            11, 12
        )

    def test_publish_gap(self):
        # in case of a gap attempt to publish all intermediate blocks from the queueing service
        # and publish current block

        self.sut.subscriber_count = MagicMock(return_value=10)
        self.sut.publish_blocks_from_queue = MagicMock()

        self.sut.publish(self.generate_raw_block_msg(10))
        msg = self.generate_raw_block_msg(20)
        self.sut.publish(msg)

        self.assertIn(msg.block_hash, self.sut.published_blocks.contents)
        self.sut.publish_blocks_from_queue.assert_called_once()
        self.sut.publish_blocks_from_queue.assert_called_with(
            11, 19
        )

    def test_publish_from_queue(self):
        self.sut.subscriber_count = MagicMock(return_value=10)
        self.sut.publish = MagicMock()

        self.sut.publish_blocks_from_queue(10, 20)
        # no blocks in queueing service
        self.sut.publish.assert_not_called()
        self.block_queuing_service.accepted_block_hash_at_height[11] = "11"
        self.sut.publish_blocks_from_queue(10, 20)
        # only one block in queueing service
        self.sut.publish.assert_called_once()
        self.sut.publish = MagicMock()
        for i in range(10, 20):
            self.block_queuing_service.accepted_block_hash_at_height[i] = str(i)
        self.sut.publish_blocks_from_queue(10, 20)
        # only one block in queueing service
        call_args = self.sut.publish.call_args_list
        self.assertTrue(10, len(call_args))
        published_blocks = set({})
        for _call in call_args:
            published_blocks.add(_call[0][0].block_number)
        self.assertEqual(set(range(10, 20)), published_blocks)
