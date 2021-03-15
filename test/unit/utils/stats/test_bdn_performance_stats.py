import time
from mock import MagicMock

from bxcommon.network.ip_endpoint import IpEndpoint
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.testing import gateway_helpers
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
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol
import bxgateway.messages.eth.eth_message_converter_factory as converter_factory
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import gateway_bdn_performance_stats_service


def _block_with_timestamp(timestamp):
    nonce = 5
    header = mock_eth_messages.get_dummy_block_header(5, int(timestamp))
    block = mock_eth_messages.get_dummy_block(nonce, header)
    return block


class GatewayBdnPerformanceStatsTest(AbstractTestCase):
    def setUp(self):
        self.node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(
                8000, include_default_eth_args=True, use_extensions=True
            ),
            block_queueing_cls=MagicMock())
        self.node.message_converter = converter_factory.create_eth_message_converter(self.node.opts)
        self.node.block_processing_service = BlockProcessingService(self.node)

        is_handshake_initiator = True
        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)
        rlpx_cipher = RLPxCipher(is_handshake_initiator, dummy_private_key, dummy_public_key)

        node_ssl_service = MockNodeSSLService(EthGatewayNode.NODE_TYPE, MagicMock())
        local_ip = "127.0.0.1"
        eth_port = 30303
        eth_opts = gateway_helpers.get_gateway_opts(
            1234, include_default_eth_args=True, blockchain_address=(local_ip, eth_port), pub_key=convert.bytes_to_hex(dummy_public_key)
        )
        self.eth_node = EthGatewayNode(eth_opts, node_ssl_service)
        self.blockchain_connection = EthNodeConnection(
            MockSocketConnection(1, node=self.node, ip_address=local_ip, port=30303), self.node)
        self.blockchain_connection.on_connection_established()
        self.node.connection_pool.add(
            19, local_ip, eth_port, self.blockchain_connection
        )

        self.blockchain_connection_1 = EthNodeConnection(
            MockSocketConnection(1, node=self.node, ip_address=local_ip, port=333), self.node)
        self.blockchain_connection_1.on_connection_established()
        self.node.mock_add_blockchain_peer(self.blockchain_connection_1)
        self.node_1_endpoint = IpEndpoint(local_ip, 333)
        self.node.connection_pool.add(
            20, self.node_1_endpoint.ip_address, self.node_1_endpoint.port, self.blockchain_connection_1
        )

        self.blockchain_connection_2 = EthNodeConnection(
            MockSocketConnection(1, node=self.node, ip_address=local_ip, port=444), self.node)
        self.blockchain_connection_2.on_connection_established()
        self.node.mock_add_blockchain_peer(self.blockchain_connection_2)
        self.node_2_endpoint = IpEndpoint(local_ip, 444)
        self.node.connection_pool.add(
            21, self.node_2_endpoint.ip_address, self.node_2_endpoint.port, self.blockchain_connection_2
        )

        self.blockchain_connection_1.network_num = 0

        self.tx_blockchain_connection_protocol = EthNodeConnectionProtocol(
            self.blockchain_connection_1, is_handshake_initiator, rlpx_cipher)
        self.block_blockchain_connection_protocol = EthNodeConnectionProtocol(
            self.blockchain_connection_1, is_handshake_initiator, rlpx_cipher)
        self.tx_blockchain_connection_protocol.publish_transaction = MagicMock()
        self.block_blockchain_connection_protocol.publish_transaction = MagicMock()

        self.tx_blockchain_connection_protocol_2 = EthNodeConnectionProtocol(
            self.blockchain_connection_2, is_handshake_initiator, rlpx_cipher)
        self.block_blockchain_connection_protocol_2 = EthNodeConnectionProtocol(
            self.blockchain_connection_2, is_handshake_initiator, rlpx_cipher)
        self.tx_blockchain_connection_protocol_2.publish_transaction = MagicMock()
        self.block_blockchain_connection_protocol_2.publish_transaction = MagicMock()

        self.relay_connection = AbstractRelayConnection(
            MockSocketConnection(1, node=self.node, ip_address=local_ip, port=12345), self.node
        )
        self.relay_connection.state = ConnectionState.INITIALIZED

        gateway_bdn_performance_stats_service.set_node(self.node)
        self.node.account_id = "12345"

    def test_bdn_stats_tx_new_full_from_bdn(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(full_message)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        for stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.values():
            self.assertEqual(1, stats.new_tx_received_from_bdn)

    def test_bdn_stats_tx_new_full_from_bdn_ignore_duplicate(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(full_message)
        self.relay_connection.msg_tx(full_message)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        for stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.values():
            self.assertEqual(1, stats.new_tx_received_from_bdn)

    def test_bdn_stats_tx_new_from_node_low_fee(self):
        self.node.opts.blockchain_networks[self.node.network_num].min_tx_network_fee = 500
        blockchain_network = self.tx_blockchain_connection_protocol.connection.node.get_blockchain_network()
        blockchain_network.protocol = "ethereum"

        txs = [
            mock_eth_messages.get_dummy_transaction(1, gas_price=5),
        ]

        tx_msg = TransactionsEthProtocolMessage(None, txs)
        self.assertEqual(1, len(tx_msg.get_transactions()))
        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(0, stats.new_tx_received_from_bdn)

        self.assertEqual(0, node_1_stats.new_tx_received_from_blockchain_node)
        self.assertEqual(
            1, node_1_stats.new_tx_received_from_blockchain_node_low_fee
        )

    def test_bdn_stats_tx_new_from_node(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(1),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_tx_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_tx_received_from_blockchain_node)

    def test_bdn_stats_tx_new_from_node_ignore_duplicate(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(1),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)
        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_tx_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_tx_received_from_blockchain_node)

    def test_bdn_stats_tx_new_from_node_ignore_duplicate_from_second_node(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(1),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.tx_blockchain_connection_protocol.msg_tx(tx_msg)
        self.tx_blockchain_connection_protocol_2.msg_tx(tx_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_tx_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_tx_received_from_blockchain_node)

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

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        for stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.values():
            self.assertEqual(1, stats.new_tx_received_from_bdn)
            self.assertEqual(0, stats.new_tx_received_from_blockchain_node)

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

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_tx_received_from_bdn)
            self.assertEqual(0, stats.new_tx_received_from_blockchain_node)
        self.assertEqual(1, node_1_stats.new_tx_received_from_blockchain_node)
        self.assertEqual(0, node_1_stats.new_tx_received_from_bdn)

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

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_tx_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_tx_received_from_blockchain_node)
        self.assertEqual(0, node_1_stats.new_tx_received_from_bdn)

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

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_blocks_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_blocks_received_from_blockchain_node)

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

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_blocks_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_blocks_received_from_blockchain_node)

    def test_bdn_stats_block_new_from_bdn(self):
        block_msg = mock_eth_messages.new_block_eth_protocol_message(21, 1017)
        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(
            internal_new_block_msg, self.node._tx_service, True, self.node.network.min_tx_age_seconds
        )
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        for stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.values():
            self.assertEqual(1, stats.new_blocks_received_from_bdn)

    def test_bdn_stats_block_new_from_bdn_ignore_duplicate(self):
        block_msg = mock_eth_messages.new_block_eth_protocol_message(21, 1017)
        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(
            internal_new_block_msg, self.node._tx_service, True, self.node.network.min_tx_age_seconds
        )
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))

        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)
        self.relay_connection.msg_broadcast(broadcast_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        for stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.values():
            self.assertEqual(1, stats.new_blocks_received_from_bdn)

    def test_bdn_stats_block_new_from_bdn_ignore_from_node(self):
        block_msg = NewBlockEthProtocolMessage(
            None,
            _block_with_timestamp(
                time.time() + 1 - (
                    self.node.opts.blockchain_ignore_block_interval_count * self.node.opts.blockchain_block_interval
                )
            ),
            10
        )

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(block_msg)
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(
            internal_new_block_msg, self.node._tx_service, True, self.node.network.min_tx_age_seconds
        )
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))
        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)

        block_msg.serialize()
        self.block_blockchain_connection_protocol.msg_block(block_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        for stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.values():
            self.assertEqual(1, stats.new_blocks_received_from_bdn)
            self.assertEqual(0, stats.new_blocks_received_from_blockchain_node)

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
        msg_bytes, block_info = self.node.message_converter.block_to_bx_block(
            internal_new_block_msg, self.node._tx_service, True, self.node.network.min_tx_age_seconds
        )
        msg_hash = Sha256Hash(crypto.double_sha256(msg_bytes))
        broadcast_msg = BroadcastMessage(message_hash=msg_hash, network_num=1, is_encrypted=False, blob=msg_bytes)
        self.relay_connection.msg_broadcast(broadcast_msg)

        self.assertEqual(3, len(gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats))
        node_1_stats = gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats[
            self.node_1_endpoint
        ]
        for endpoint, stats in gateway_bdn_performance_stats_service.interval_data.blockchain_node_to_bdn_stats.items():
            if endpoint == self.node_1_endpoint:
                continue
            self.assertEqual(1, stats.new_blocks_received_from_bdn)

        self.assertEqual(1, node_1_stats.new_blocks_received_from_blockchain_node)
        self.assertEqual(0, node_1_stats.new_blocks_received_from_bdn)
