import time
from mock import MagicMock

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.utils import crypto, convert
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.utils.eth import crypto_utils
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service


def _block_with_timestamp(timestamp):
    nonce = 5
    header = mock_eth_messages.get_dummy_block_header(5, int(timestamp))
    block = mock_eth_messages.get_dummy_block(nonce, header)
    return block


class GatewayTransactionStatsServiceTest(AbstractTestCase):
    def setUp(self):
        self.node = MockGatewayNode(helpers.get_gateway_opts(8000, include_default_eth_args=True),
                                    block_queueing_cls=MagicMock())
        self.node.message_converter = EthMessageConverter()
        self.node.block_processing_service = BlockProcessingService(self.node)

        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)
        node_ssl_service = MockNodeSSLService(EthGatewayNode.NODE_TYPE, MagicMock())
        eth_opts = helpers.get_gateway_opts(1234, include_default_eth_args=True,
                                            pub_key=convert.bytes_to_hex(dummy_public_key))
        self.eth_node = EthGatewayNode(eth_opts, node_ssl_service)
        self.node.node_conn = EthNodeConnection(
            MockSocketConnection(node=self.node, ip_address="127.0.0.1", port=12345), self.eth_node)

        self.blockchain_connection = EthBaseConnection(
            MockSocketConnection(node=self.node, ip_address="127.0.0.1", port=333), self.node)
        self.blockchain_connection.state = ConnectionState.ESTABLISHED
        self.node.node_conn = self.blockchain_connection

        self.blockchain_connection.network_num = 0

        self.tx_blockchain_connection_protocol = EthNodeConnectionProtocol(
            self.blockchain_connection, True, dummy_private_key, dummy_public_key)
        self.block_blockchain_connection_protocol = EthBaseConnectionProtocol(
            self.blockchain_connection, True, dummy_private_key, dummy_public_key)

        self.relay_connection = AbstractRelayConnection(
            MockSocketConnection(node=self.node, ip_address="127.0.0.1", port=12345), self.node
        )
        self.relay_connection.state = ConnectionState.INITIALIZED


        gateway_bdn_performance_stats_service.set_node(self.node)

    def test_bdn_stats_tx_new_full_from_bdn(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(full_message)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_bdn)

    def test_bdn_stats_tx_new_full_from_bdn_ignore_duplicate(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(full_message)
        self.relay_connection.msg_tx(full_message)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_bdn)

    def test_bdn_stats_tx_new_from_node(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(1),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_blockchain_node)

    def test_bdn_stats_tx_new_from_node_ignore_duplicate(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(1),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)
        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_blockchain_node)

    def test_bdn_stats_tx_new_full_from_bdn_ignore_from_node(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)
        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1, tx_val=tx_bytes)
            self.relay_connection.msg_tx(relay_full_message)

        self.tx_blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_bdn)
        self.assertEqual(0, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_blockchain_node)

    def test_bdn_stats_tx_new_from_node_ignore_from_bdn(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)

        self.tx_blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)
        time.time = MagicMock(return_value=time.time() + 1)
        self.node.alarm_queue.fire_alarms()
        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1, tx_val=tx_bytes)
            self.relay_connection.msg_tx(relay_full_message)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_blockchain_node)
        self.assertEqual(0, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_bdn)

    def test_bdn_stats_tx_ignore_new_compact_from_bdn_log_new_from_node(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)
        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1)
            self.relay_connection.msg_tx(relay_compact_message)

        self.tx_blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)

        self.assertEqual(0, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_bdn)
        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_tx_received_from_blockchain_node)

    def test_bdn_stats_block_new_from_node(self):
        block_msg = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() + 1 - self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
            ),
            10
        )
        block_msg.serialize()
        self.block_blockchain_connection_protocol.msg_block(block_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_blockchain_node)

    def test_bdn_stats_block_new_from_node_ignore_duplicate(self):
        block_msg = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() + 1 - self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
            ),
            10
        )
        block_msg.serialize()
        self.block_blockchain_connection_protocol.msg_block(block_msg)
        self.block_blockchain_connection_protocol.msg_block(block_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_blockchain_node)

    def test_bdn_stats_block_new_from_bdn(self):
        block_msg = mock_eth_messages.new_block_eth_protocol_message(21, 1017)
        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(internal_new_block_msg,
                                                                              self.node._tx_service)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_bdn)

    def test_bdn_stats_block_new_from_bdn_ignore_duplicate(self):
        block_msg = mock_eth_messages.new_block_eth_protocol_message(21, 1017)
        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(internal_new_block_msg,
                                                                              self.node._tx_service)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)
        self.relay_connection.msg_broadcast(broadcast_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_bdn)

    def test_bdn_stats_block_new_from_bdn_ignore_from_node(self):
        block_msg = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() + 1 - self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
            ),
            10
        )

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(internal_new_block_msg,
                                                                              self.node._tx_service)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))
        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)

        block_msg.serialize()
        self.block_blockchain_connection_protocol.msg_block(block_msg)

        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_bdn)
        self.assertEqual(0, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_blockchain_node)

    def test_bdn_stats_block_new_from_node_ignore_from_bdn(self):
        block_msg = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() + 1 - self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
            ),
            10
        )

        block_msg.serialize()
        self.block_blockchain_connection_protocol.msg_block(block_msg)

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(internal_new_block_msg,
                                                                              self.node._tx_service)
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))
        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)

        self.assertEqual(0, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_bdn)
        self.assertEqual(1, gateway_bdn_performance_stats_service.interval_data.new_blocks_received_from_blockchain_node)
