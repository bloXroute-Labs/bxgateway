from typing import cast

from mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import DEFAULT_NETWORK_NUM, LOCALHOST
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.models.transaction_info import TransactionInfo
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.crypto import symmetric_encrypt, SHA256_HASH_LEN
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.services.extension_transaction_service import ExtensionTransactionService

from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
import bxgateway.messages.btc.btc_message_converter_factory as converter_factory
from bxgateway.messages.btc.inventory_btc_message import InvBtcMessage, InventoryType
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service


class BtcRelayConnectionTest(AbstractTestCase):
    BTC_HASH = BtcObjectHash(crypto.double_sha256(b"123"), length=SHA256_HASH_LEN)

    MAGIC = 12345
    VERSION = 23456
    TEST_NETWORK_NUM = 12345

    def setUp(self):
        opts = helpers.get_gateway_opts(8000, include_default_btc_args=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism(opts.thread_pool_parallelism_degree)
        self.gateway_node = BtcGatewayNode(opts)
        self.sut = BtcRelayConnection(MockSocketConnection(), (LOCALHOST, 8001), self.gateway_node)
        self.gateway_node.node_conn = MockConnection(MockSocketConnection(1), (LOCALHOST, 8002), self.gateway_node)
        self.gateway_node.node_conn.message_converter = converter_factory.create_btc_message_converter(
            12345, self.gateway_node.opts
        )
        self.gateway_node.node_conn.state = ConnectionState.ESTABLISHED

        self.gateway_node.send_msg_to_node = MagicMock()
        self.sut.enqueue_msg = MagicMock()
        gateway_transaction_stats_service.set_node(self.gateway_node)

    def btc_transactions(self):
        return [TxBtcMessage(self.MAGIC, self.VERSION, [], [], i) for i in range(10)]

    def btc_transactions_bytes(self):
        return [transaction.rawbytes()[BTC_HDR_COMMON_OFF:] for transaction in self.btc_transactions()]

    def btc_block(self, txns=None):
        if txns is None:
            txns = self.btc_transactions_bytes()
        return BlockBtcMessage(self.MAGIC, self.VERSION, self.BTC_HASH, self.BTC_HASH, 0, 0, 0, txns)

    def bx_block(self, btc_block=None):
        if btc_block is None:
            btc_block = self.btc_block()
        return bytes(self.sut.message_converter.block_to_bx_block(btc_block, self.gateway_node.get_tx_service())[0])

    def bx_transactions(self, transactions=None, assign_short_ids=False):
        if transactions is None:
            transactions = self.btc_transactions()

        bx_transactions = []
        for i, transaction in enumerate(transactions):
            transaction = self.sut.message_converter.tx_to_bx_txs(transaction, self.TEST_NETWORK_NUM)[0][0]
            if assign_short_ids:
                transaction._short_id = i + 1  # 0 is null SID
            bx_transactions.append(transaction)
        return bx_transactions

    def test_msg_broadcast_wait_for_key(self):
        btc_block = self.btc_block()
        bx_block = self.bx_block(btc_block)

        key, ciphertext = symmetric_encrypt(bx_block)
        block_hash = crypto.double_sha256(ciphertext)
        broadcast_message = BroadcastMessage(Sha256Hash(block_hash), self.TEST_NETWORK_NUM, True, ciphertext)

        self.sut.msg_broadcast(broadcast_message)

        # handle duplicate messages
        self.sut.msg_broadcast(broadcast_message)
        self.sut.msg_broadcast(broadcast_message)

        self.gateway_node.send_msg_to_node.assert_not_called()
        self.assertEqual(1, len(self.gateway_node.in_progress_blocks))

        key_message = KeyMessage(Sha256Hash(block_hash), self.TEST_NETWORK_NUM, key)
        self.sut.msg_key(key_message)

        self._assert_block_sent(btc_block)

    def test_msg_broadcast_duplicate_block_with_different_short_id(self):
        # test scenario when first received block is compressed with unknown short ids,
        # but second received block is compressed with known short ids
        btc_block = self.btc_block()
        block_hash = btc_block.block_hash()
        transactions = self.bx_transactions()

        unknown_sid_transaction_service = ExtensionTransactionService(MockNode(helpers.get_gateway_opts(8999)), 0)
        for i, transaction in enumerate(transactions):
            unknown_sid_transaction_service.assign_short_id(transaction.tx_hash(), i)
            unknown_sid_transaction_service.set_transaction_contents(transaction.tx_hash(), transaction.tx_val())

        unknown_short_id_block = bytes(self.sut.message_converter.block_to_bx_block(btc_block,
                                                                                    unknown_sid_transaction_service)[0])
        unknown_key, unknown_cipher = symmetric_encrypt(unknown_short_id_block)
        unknown_block_hash = crypto.double_sha256(unknown_cipher)
        unknown_message = BroadcastMessage(Sha256Hash(unknown_block_hash), self.TEST_NETWORK_NUM, True, unknown_cipher)
        unknown_key_message = KeyMessage(Sha256Hash(unknown_block_hash), self.TEST_NETWORK_NUM, unknown_key)

        local_transaction_service = self.gateway_node.get_tx_service()
        for i, transaction in enumerate(transactions):
            local_transaction_service.assign_short_id(transaction.tx_hash(), i + 20)
            local_transaction_service.set_transaction_contents(transaction.tx_hash(), transaction.tx_val())

        known_short_id_block = bytes(self.sut.message_converter.block_to_bx_block(btc_block,
                                                                                  local_transaction_service)[0])
        known_key, known_cipher = symmetric_encrypt(known_short_id_block)
        known_block_hash = crypto.double_sha256(known_cipher)
        known_message = BroadcastMessage(Sha256Hash(known_block_hash), self.TEST_NETWORK_NUM, True, known_cipher)
        known_key_message = KeyMessage(Sha256Hash(known_block_hash), self.TEST_NETWORK_NUM, known_key)

        self.sut.msg_broadcast(unknown_message)
        self.sut.msg_key(unknown_key_message)

        self.assertEqual(1, len(self.gateway_node.block_queuing_service))
        self.assertEqual(True, self.gateway_node.block_queuing_service._blocks[block_hash][0])
        self.assertEqual(1, len(self.gateway_node.block_recovery_service._block_hash_to_bx_block_hashes))
        self.assertNotIn(block_hash, self.gateway_node.blocks_seen.contents)

        self.sut.msg_broadcast(known_message)
        self.sut.msg_key(known_key_message)

        self.assertEqual(0, len(self.gateway_node.block_queuing_service))
        self.assertEqual(0, len(self.gateway_node.block_recovery_service._block_hash_to_bx_block_hashes))
        self.assertIn(block_hash, self.gateway_node.blocks_seen.contents)

    def test_msg_key_wait_for_broadcast(self):
        btc_block = self.btc_block()
        bx_block = self.bx_block(btc_block)

        key, ciphertext = symmetric_encrypt(bx_block)
        block_hash = crypto.double_sha256(ciphertext)

        self.gateway_node.send_msg_to_node.assert_not_called()

        key_message = KeyMessage(Sha256Hash(block_hash), self.TEST_NETWORK_NUM, key)
        self.sut.msg_key(key_message)

        # handle duplicate broadcasts
        self.sut.msg_key(key_message)
        self.sut.msg_key(key_message)

        self.assertEqual(1, len(self.gateway_node.in_progress_blocks))

        broadcast_message = BroadcastMessage(Sha256Hash(block_hash), self.TEST_NETWORK_NUM, True, ciphertext)
        self.sut.msg_broadcast(broadcast_message)

        self._assert_block_sent(btc_block)

    def test_msg_tx(self):
        transactions = self.bx_transactions(assign_short_ids=True)
        for transaction in transactions:
            self.sut.msg_tx(transaction)

        for i, transaction in enumerate(transactions):
            transaction_hash = transaction.tx_hash()
            self.assertTrue(self.gateway_node.get_tx_service().has_transaction_contents(transaction_hash))
            self.assertTrue(self.gateway_node.get_tx_service().has_transaction_short_id(transaction_hash))
            self.assertEqual(i + 1, self.gateway_node.get_tx_service().get_short_id(transaction_hash))

            stored_hash, stored_content, _ = self.gateway_node.get_tx_service().get_transaction(i + 1)
            self.assertEqual(transaction_hash, stored_hash)
            self.assertEqual(transaction.tx_val(), stored_content)

        self.assertEqual(len(transactions), self.gateway_node.send_msg_to_node.call_count)

    def test_msg_tx_additional_sid(self):
        transactions = self.bx_transactions(assign_short_ids=True)
        for transaction in transactions:
            self.sut.msg_tx(transaction)

        # rebroadcast transactions with more sids
        for i, transaction in enumerate(transactions):
            transaction._short_id += 20
            self.sut.msg_tx(transaction)

        for i, transaction in enumerate(transactions):
            transaction_hash = transaction.tx_hash()
            self.assertTrue(self.gateway_node.get_tx_service().has_transaction_contents(transaction_hash))
            self.assertTrue(self.gateway_node.get_tx_service().has_transaction_short_id(transaction_hash))

            stored_hash, stored_content, _ = self.gateway_node.get_tx_service().get_transaction(i + 1)
            self.assertEqual(transaction_hash, stored_hash)
            self.assertEqual(transaction.tx_val(), stored_content)

            stored_hash2, stored_content2, _ = self.gateway_node.get_tx_service().get_transaction(i + 21)
            self.assertEqual(transaction_hash, stored_hash2)
            self.assertEqual(transaction.tx_val(), stored_content2)

        # only 10 times even with rebroadcast SID
        self.assertEqual(len(transactions), self.gateway_node.send_msg_to_node.call_count)

    def test_msg_tx_duplicate_ignore(self):
        transactions = self.bx_transactions(assign_short_ids=True)
        for transaction in transactions:
            self.sut.msg_tx(transaction)

        for transaction in transactions:
            self.sut.msg_tx(transaction)

        self.assertEqual(len(transactions), self.gateway_node.send_msg_to_node.call_count)

    def test_get_txs_block_recovery(self):
        btc_block = self.btc_block()
        transactions = self.btc_transactions()

        # assign short ids that the local connection won't know about until it gets the txs message
        remote_transaction_service = ExtensionTransactionService(MockNode(helpers.get_gateway_opts(8999)), 0)
        short_id_mapping = {}
        for i, transaction in enumerate(transactions):
            remote_transaction_service.assign_short_id(transaction.tx_hash(), i + 1)
            remote_transaction_service.set_transaction_contents(transaction.tx_hash(), transaction.tx())
            short_id_mapping[transaction.tx_hash()] = TransactionInfo(transaction.tx_hash(), transaction.tx(), i + 1)

        bx_block = bytes(self.sut.message_converter.block_to_bx_block(btc_block, remote_transaction_service)[0])

        self.gateway_node.block_recovery_service.add_block = \
            MagicMock(wraps=self.gateway_node.block_recovery_service.add_block)
        self.gateway_node.send_msg_to_node = MagicMock()
        self.gateway_node.broadcast = MagicMock()

        key, ciphertext = symmetric_encrypt(bx_block)
        block_hash = crypto.double_sha256(ciphertext)
        key_message = KeyMessage(Sha256Hash(block_hash), DEFAULT_NETWORK_NUM, key)
        broadcast_message = BroadcastMessage(Sha256Hash(block_hash), DEFAULT_NETWORK_NUM, True, ciphertext)

        self.sut.msg_broadcast(broadcast_message)

        self.gateway_node.broadcast.reset_mock()
        self.sut.msg_key(key_message)

        self.gateway_node.block_recovery_service.add_block.assert_called_once()
        self.assertEqual(2, self.gateway_node.broadcast.call_count)

        recovery_broadcast = self.gateway_node.broadcast.call_args_list[0]
        ((gettxs_message,), recovery_kwargs) = recovery_broadcast
        self.assertIsInstance(gettxs_message, GetTxsMessage)
        self.assertIn(ConnectionType.RELAY_TRANSACTION, recovery_kwargs["connection_types"])

        key_broadcast = self.gateway_node.broadcast.call_args_list[1]
        ((key_message, _conn), recovery_kwargs) = key_broadcast
        self.assertIsInstance(key_message, KeyMessage)
        self.assertIn(ConnectionType.GATEWAY, recovery_kwargs["connection_types"])

        txs = [tx for tx in short_id_mapping.values()]
        txs_message = TxsMessage(txs=txs)
        self.sut.msg_txs(txs_message)

        self._assert_block_sent(btc_block)

    def test_get_txs_multiple_sid_assignments(self):
        transactions = self.btc_transactions()

        # assign short ids that the local connection won't know about until it gets the txs message
        remote_transaction_service1 = ExtensionTransactionService(MockNode(helpers.get_gateway_opts(8999)), 0)
        short_id_mapping1 = {}
        for i, transaction in enumerate(transactions):
            remote_transaction_service1.assign_short_id(transaction.tx_hash(), i + 1)
            remote_transaction_service1.set_transaction_contents(transaction.tx_hash(), transaction.tx())
            short_id_mapping1[transaction.tx_hash()] = TransactionInfo(transaction.tx_hash(), transaction.tx(), i + 1)

        txs_message_1 = TxsMessage([tx for tx in short_id_mapping1.values()])
        self.sut.msg_txs(txs_message_1)

        for transaction_hash, tx_info in short_id_mapping1.items():
            self.assertEqual(tx_info.short_id, self.gateway_node.get_tx_service().get_short_id(transaction_hash))
            stored_hash, stored_content, _ = self.gateway_node.get_tx_service().get_transaction(tx_info.short_id)
            self.assertEqual(transaction_hash, stored_hash)
            self.assertEqual(tx_info.contents, stored_content)

        remote_transaction_service2 = ExtensionTransactionService(MockNode(helpers.get_gateway_opts(8999)), 0)
        short_id_mapping2 = {}
        for i, transaction in enumerate(transactions):
            remote_transaction_service2.assign_short_id(transaction.tx_hash(), i + 101)
            remote_transaction_service2.set_transaction_contents(transaction.tx_hash(), transaction.tx())
            short_id_mapping2[transaction.tx_hash()] = TransactionInfo(transaction.tx_hash(), transaction.tx(), i + 101)

        txs_message_2 = TxsMessage([tx for tx in short_id_mapping2.values()])
        self.sut.msg_txs(txs_message_2)

        for transaction_hash, tx_info in short_id_mapping2.items():
            stored_hash, stored_content, _ = self.gateway_node.get_tx_service().get_transaction(tx_info.short_id)
            self.assertEqual(transaction_hash, stored_hash)
            self.assertEqual(tx_info.contents, stored_content)

    def _assert_block_sent(self, btc_block):
        self.gateway_node.send_msg_to_node.assert_called()
        calls = self.gateway_node.send_msg_to_node.call_args_list

        self.assertEqual(2, len(calls))

        ((sent_block_msg,), _) = calls[0]
        self.assertEqual(btc_block.version(), sent_block_msg.version())
        self.assertEqual(btc_block.magic(), sent_block_msg.magic())
        self.assertEqual(btc_block.prev_block_hash(), sent_block_msg.prev_block_hash())
        self.assertEqual(btc_block.merkle_root(), sent_block_msg.merkle_root())
        self.assertEqual(btc_block.timestamp(), sent_block_msg.timestamp())
        self.assertEqual(btc_block.bits(), sent_block_msg.bits())
        self.assertEqual(btc_block.nonce(), sent_block_msg.nonce())
        self.assertEqual(btc_block.txn_count(), sent_block_msg.txn_count())

        ((sent_inv_msg,), _) = calls[1]
        self.assertIsInstance(sent_inv_msg, InvBtcMessage)
        sent_inv_msg = cast(InvBtcMessage, sent_inv_msg)

        inv_items = list(iter(sent_inv_msg))
        self.assertEqual(1, len(inv_items))

        block_inv = inv_items[0]
        self.assertEqual(InventoryType.MSG_BLOCK, block_inv[0])
        self.assertEqual(btc_block.block_hash(), block_inv[1])
