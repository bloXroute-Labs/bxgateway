from argparse import Namespace

from mock import MagicMock

from bxcommon.constants import BTC_HDR_COMMON_OFF, NULL_TX_SID, DEFAULT_NETWORK_NUM
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.messages.btc.block_btc_message import BlockBTCMessage
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import get_gateway_opts
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.crypto import symmetric_encrypt, SHA256_HASH_LEN
from bxcommon.utils.object_hash import ObjectHash, BTCObjectHash
from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection


class BtcRelayConnectionTest(AbstractTestCase):
    BTC_HASH = BTCObjectHash(crypto.double_sha256("123"), length=SHA256_HASH_LEN)

    MAGIC = 12345
    VERSION = 23456
    TEST_NETWORK_NUM = 12345

    def setUp(self):
        self.gateway_node = BtcGatewayNode(get_gateway_opts(8000, include_default_btc_args=True))
        self.sut = BtcRelayConnection(MockSocketConnection(), ("127.0.0.1", 8001), self.gateway_node)

        self.gateway_node.tx_service = MagicMock()

        # no shortids by default
        def get_txid(_txhash):
            return NULL_TX_SID

        def get_tx_from_sid(_sid):
            return None, None

        self.gateway_node.tx_service.get_txid = get_txid
        self.gateway_node.tx_service.get_tx_from_sid = get_tx_from_sid

    def transactions(self):
        return [TxBTCMessage(self.MAGIC, self.VERSION, [], [], i) for i in xrange(10)]

    def transactions_bytes(self):
        return [transaction.rawbytes()[BTC_HDR_COMMON_OFF:] for transaction in self.transactions()]

    def block(self, txns):
        btc_block = BlockBTCMessage(self.MAGIC, self.VERSION, self.BTC_HASH, self.BTC_HASH, 0, 0, 0, txns)
        return btc_block, bytes(
            self.sut.message_converter.block_to_bx_block(btc_block, self.gateway_node.get_tx_service()))

    def test_msg_broadcast_wait_for_key(self):

        btc_block, blx_block = self.block(self.transactions_bytes())
        self.gateway_node.send_msg_to_node = MagicMock()

        key, ciphertext = symmetric_encrypt(blx_block)
        block_hash = crypto.double_sha256(ciphertext)
        broadcast_message = BroadcastMessage(ObjectHash(block_hash),  self.TEST_NETWORK_NUM, ciphertext)

        self.sut.msg_broadcast(broadcast_message)

        # handle duplicate messages
        self.sut.msg_broadcast(broadcast_message)
        self.sut.msg_broadcast(broadcast_message)

        self.gateway_node.send_msg_to_node.assert_not_called()
        self.assertEqual(1, len(self.gateway_node.in_progress_blocks))

        key_message = KeyMessage(ObjectHash(block_hash), key, self.TEST_NETWORK_NUM)
        self.sut.msg_key(key_message)

        self.gateway_node.send_msg_to_node.assert_called_once()
        ((sent_msg,), _) = self.gateway_node.send_msg_to_node.call_args

        self.assertEqual(btc_block.version(), sent_msg.version())
        self.assertEqual(btc_block.magic(), sent_msg.magic())
        self.assertEqual(btc_block.prev_block(), sent_msg.prev_block())
        self.assertEqual(btc_block.merkle_root(), sent_msg.merkle_root())
        self.assertEqual(btc_block.timestamp(), sent_msg.timestamp())
        self.assertEqual(btc_block.bits(), sent_msg.bits())
        self.assertEqual(btc_block.nonce(), sent_msg.nonce())
        self.assertEqual(btc_block.txn_count(), sent_msg.txn_count())

    def test_msg_key_wait_for_broadcast(self):
        btc_block, blx_block = self.block(self.transactions_bytes())
        self.gateway_node.send_msg_to_node = MagicMock()

        key, ciphertext = symmetric_encrypt(blx_block)
        block_hash = crypto.double_sha256(ciphertext)

        self.gateway_node.send_msg_to_node.assert_not_called()

        key_message = KeyMessage(ObjectHash(block_hash), key, self.TEST_NETWORK_NUM)
        self.sut.msg_key(key_message)

        # handle duplicate broadcasts
        self.sut.msg_key(key_message)
        self.sut.msg_key(key_message)

        self.assertEqual(1, len(self.gateway_node.in_progress_blocks))

        broadcast_message = BroadcastMessage(ObjectHash(block_hash), self.TEST_NETWORK_NUM, ciphertext)
        self.sut.msg_broadcast(broadcast_message)

        self.gateway_node.send_msg_to_node.assert_called_once()
        ((sent_msg,), _) = self.gateway_node.send_msg_to_node.call_args

        self.assertEqual(btc_block.version(), sent_msg.version())
        self.assertEqual(btc_block.magic(), sent_msg.magic())
        self.assertEqual(btc_block.prev_block(), sent_msg.prev_block())
        self.assertEqual(btc_block.merkle_root(), sent_msg.merkle_root())
        self.assertEqual(btc_block.timestamp(), sent_msg.timestamp())
        self.assertEqual(btc_block.bits(), sent_msg.bits())
        self.assertEqual(btc_block.nonce(), sent_msg.nonce())
        self.assertEqual(btc_block.txn_count(), sent_msg.txn_count())

    def test_get_txs_block_recovery(self):
        transactions = self.transactions()

        sid_mapping = {}

        def get_txid(txhash):
            if len(sid_mapping) < len(transactions):
                short_id = len(sid_mapping)
                transaction = transactions[short_id].tx()
                sid_mapping[txhash] = (short_id, txhash, transaction)
                return short_id
            else:
                # so msg_txs assigns tx to sids
                return NULL_TX_SID

        # simulating connection not knowing about sids mappings until gettxs message is sent
        known_sid_mapping = {}

        def get_tx_from_sid(sid):
            if sid in known_sid_mapping:
                return known_sid_mapping[sid]
            else:
                return None, None

        def assign_tx_to_sid(tx_hash, sid, _tx_time):
            known_sid_mapping[sid] = (tx_hash, sid_mapping[tx_hash][2])

        tx_service = self.gateway_node.get_tx_service()

        tx_service.get_txid = get_txid
        tx_service.get_tx_from_sid = get_tx_from_sid
        tx_service.assign_tx_to_sid = assign_tx_to_sid

        self.gateway_node.block_recovery_service.add_block = \
            MagicMock(wraps=self.gateway_node.block_recovery_service.add_block)
        self.gateway_node.send_msg_to_node = MagicMock()

        btc_block, blx_block = self.block(self.transactions_bytes())

        key, ciphertext = symmetric_encrypt(blx_block)
        block_hash = crypto.double_sha256(ciphertext)
        key_message = KeyMessage(ObjectHash(block_hash), key, DEFAULT_NETWORK_NUM)
        broadcast_message = BroadcastMessage(ObjectHash(block_hash), DEFAULT_NETWORK_NUM, ciphertext)

        self.sut.msg_broadcast(broadcast_message)
        self.sut.msg_key(key_message)

        self.gateway_node.block_recovery_service.add_block.assert_called_once()

        txs = [tx for tx in sid_mapping.values()]
        txs_message = TxsMessage(txs=txs)
        self.sut.msg_txs(txs_message)

        self.gateway_node.send_msg_to_node.assert_called_once()
        ((sent_msg,), _) = self.gateway_node.send_msg_to_node.call_args
        sent_btc_block = sent_msg

        self.assertEqual(btc_block.version(), sent_btc_block.version())
        self.assertEqual(btc_block.magic(), sent_btc_block.magic())
        self.assertEqual(btc_block.prev_block(), sent_btc_block.prev_block())
        self.assertEqual(btc_block.merkle_root(), sent_btc_block.merkle_root())
        self.assertEqual(btc_block.timestamp(), sent_btc_block.timestamp())
        self.assertEqual(btc_block.bits(), sent_btc_block.bits())
        self.assertEqual(btc_block.nonce(), sent_btc_block.nonce())
        self.assertEqual(btc_block.txn_count(), sent_btc_block.txn_count())
