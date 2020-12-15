import time
from mock import MagicMock

from bxgateway.testing import gateway_helpers
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.eth.eth_base_connection import EthBaseConnection
from bxgateway.connections.eth.eth_node_connection_protocol import EthNodeConnectionProtocol
import bxgateway.messages.eth.eth_message_converter_factory as converter_factory
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import TransactionsEthProtocolMessage


class GatewayTransactionStatsServiceTest(AbstractTestCase):

    def setUp(self):
        self.node = MockGatewayNode(gateway_helpers.get_gateway_opts(
            8000, include_default_btc_args=True, include_default_eth_args=True)
        )

        self.relay_connection = AbstractRelayConnection(
            MockSocketConnection(node=self.node, ip_address="127.0.0.1", port=12345), self.node
        )
        self.blockchain_connection = EthBaseConnection(
            MockSocketConnection(node=self.node, ip_address="127.0.0.1", port=1234), self.node)
        self.blockchain_connection2 = EthBaseConnection(
            MockSocketConnection(node=self.node, ip_address="127.0.0.1", port=4321), self.node)
        self.node.message_converter = converter_factory.create_eth_message_converter(self.node.opts)

        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        dummy_public_key = crypto_utils.private_to_public_key(dummy_private_key)
        self.blockchain_connection_protocol = EthNodeConnectionProtocol(
            self.blockchain_connection, True, dummy_private_key, dummy_public_key)
        self.blockchain_connection.network_num = 0
        self.blockchain_connection_protocol.publish_transaction = MagicMock()

        self.blockchain_connection_protocol2 = EthNodeConnectionProtocol(
            self.blockchain_connection2, True, dummy_private_key, dummy_public_key)
        self.blockchain_connection2.network_num = 0
        self.blockchain_connection_protocol2.publish_transaction = MagicMock()

        self.relay_connection.state = ConnectionState.INITIALIZED
        gateway_transaction_stats_service.set_node(self.node)

    def test_tx_stats_new_full_from_relay(self):
        short_id = 123
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(full_message)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)

    def test_tx_stats_new_compact_from_relay(self):
        short_id = 321
        tx_hash = helpers.generate_object_hash()

        compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)

        self.relay_connection.msg_tx(compact_message)

        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)

    def test_tx_stats_duplicate_full_from_relay(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(full_message)
        self.relay_connection.msg_tx(full_message)

        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_duplicate_compact_from_relay(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()

        compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)

        self.relay_connection.msg_tx(compact_message)
        self.relay_connection.msg_tx(compact_message)

        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.duplicate_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_duplicate_compact_after_full_from_relay(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)
        compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)

        self.relay_connection.msg_tx(full_message)
        self.relay_connection.msg_tx(compact_message)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_new_full_after_new_compact_from_relay(self):
        short_id = 1
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)
        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)

        self.relay_connection.msg_tx(compact_message)
        self.relay_connection.msg_tx(full_message)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_redundant_tx_contents_from_relay(self):
        tx_hash = helpers.generate_object_hash()
        tx_content = helpers.generate_bytearray(250)

        short_id = 1
        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)
        self.relay_connection.msg_tx(full_message)

        short_id = 2
        full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id, tx_val=tx_content)
        self.relay_connection.msg_tx(full_message)

        self.assertEqual(2, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_compact_transactions_received_from_relays)
        self.assertEqual(2, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_multiple_new_compact_from_relay(self):
        tx_hash = helpers.generate_object_hash()

        short_id = 1
        compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)
        self.relay_connection.msg_tx(compact_message)

        short_id = 2
        compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=short_id)
        self.relay_connection.msg_tx(compact_message)

        self.assertEqual(0, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(2, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_compact_transactions_received_from_relays)
        self.assertEqual(2, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_new_from_blockchain_node(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(1),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_transactions_received_from_blockchain)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_transactions_received_from_blockchain)

    def test_tx_stats_duplicate_from_blockchain_node(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.blockchain_connection_protocol.msg_tx(tx_msg)
        self.blockchain_connection_protocol.msg_tx(tx_msg)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_transactions_received_from_blockchain)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.duplicate_transactions_received_from_blockchain)

    def test_tx_stats_duplicate_from_different_blockchain_node(self):
        txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        tx_msg = TransactionsEthProtocolMessage(None, txs)

        self.blockchain_connection_protocol.msg_tx(tx_msg)
        self.blockchain_connection_protocol2.msg_tx(tx_msg)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_transactions_received_from_blockchain)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.duplicate_transactions_received_from_blockchain)
        
    def test_tx_stats_new_full_relay_duplicate_blockchain_node(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)
        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1, tx_val=tx_bytes)
            self.relay_connection.msg_tx(relay_full_message)

        self.blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.duplicate_transactions_received_from_blockchain)

    def test_tx_stats_new_compact_relay_new_blockchain_node(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)
        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1)
            self.relay_connection.msg_tx(relay_compact_message)

        self.blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_transactions_received_from_blockchain)

    def test_tx_stats_new_blockchain_node_redundant_full_relay(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)

        self.blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)

        time.time = MagicMock(return_value=time.time() + 1)
        self.node.alarm_queue.fire_alarms()

        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_full_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1, tx_val=tx_bytes)
            self.relay_connection.msg_tx(relay_full_message)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_full_transactions_received_from_relays)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.duplicate_full_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_transactions_received_from_blockchain)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)

    def test_tx_stats_new_blockchain_node_new_compact_relay(self):
        blockchain_node_txs = [
            mock_eth_messages.get_dummy_transaction(7),
        ]
        blockchain_node_tx_msg = TransactionsEthProtocolMessage(None, blockchain_node_txs)

        self.blockchain_connection_protocol.msg_tx(blockchain_node_tx_msg)

        bx_tx_message = self.node.message_converter.tx_to_bx_txs(blockchain_node_tx_msg, 1)
        for (msg, tx_hash, tx_bytes) in bx_tx_message:
            relay_compact_message = TxMessage(message_hash=tx_hash, network_num=1, short_id=1)
            self.relay_connection.msg_tx(relay_compact_message)

        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_transactions_received_from_blockchain)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.new_compact_transactions_received_from_relays)
        self.assertEqual(1, gateway_transaction_stats_service.interval_data.short_id_assignments_processed)
        self.assertEqual(0, gateway_transaction_stats_service.interval_data.redundant_transaction_content_messages)
