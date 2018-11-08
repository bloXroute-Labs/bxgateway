from argparse import Namespace

from mock import MagicMock

from bxcommon.constants import BTC_HDR_COMMON_OFF, NULL_TX_SID
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.messages.btc.block_btc_message import BlockBTCMessage
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.crypto import symmetric_encrypt, SHA256_HASH_LEN
from bxcommon.utils.object_hash import ObjectHash, BTCObjectHash
from bxgateway.connections.gateway_node import GatewayNode
from bxgateway.connections.relay_connection import RelayConnection
from bxgateway.messages import btc_message_parser


class RelayConnectionTest(AbstractTestCase):
    BTC_HASH = BTCObjectHash(crypto.double_sha256("123"), length=SHA256_HASH_LEN)

    def setUp(self):
        opts = Namespace()
        opts.__dict__ = {
            "sid_expire_time": 0,
            "external_port": 8000,
            "test_mode": []
        }
        self.gateway_node = GatewayNode(opts)
        self.sut = RelayConnection(MockSocketConnection(), ("127.0.0.1", 8001), self.gateway_node)

        self.gateway_node.tx_service = MagicMock()

        # no shortids for this test yet
        def get_txid(_txhash):
            return NULL_TX_SID

        def get_tx_from_sid(_sid):
            return None, None

        self.gateway_node.tx_service.get_txid = get_txid
        self.gateway_node.tx_service.get_tx_from_sid = get_tx_from_sid

    def block(self):
        magic = 12345
        version = 23456
        txns = [TxBTCMessage(magic, version, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]
        btc_block = BlockBTCMessage(magic, version, self.BTC_HASH, self.BTC_HASH, 0, 0, 0, txns)
        return btc_block, bytes(
            btc_message_parser.btc_block_to_bloxroute_block(btc_block, self.gateway_node.tx_service))

    def test_msg_broadcast_wait_for_key(self):
        btc_block, message = self.block()
        self.gateway_node.send_bytes_to_node = MagicMock()

        key, ciphertext = symmetric_encrypt(message)
        block_hash = crypto.double_sha256(ciphertext)
        broadcast_message = BroadcastMessage(ObjectHash(block_hash), ciphertext)

        self.sut.msg_broadcast(broadcast_message)

        # handle duplicate messages
        self.sut.msg_broadcast(broadcast_message)
        self.sut.msg_broadcast(broadcast_message)

        self.gateway_node.send_bytes_to_node.assert_not_called()
        self.assertEqual(1, len(self.gateway_node.in_progress_blocks))

        key_message = KeyMessage(ObjectHash(block_hash), key)
        self.sut.msg_key(key_message)

        self.gateway_node.send_bytes_to_node.assert_called_once()
        ((sent_msg,), _) = self.gateway_node.send_bytes_to_node.call_args
        sent_btc_block = BlockBTCMessage(buf=bytearray(sent_msg))

        self.assertEqual(btc_block.version(), sent_btc_block.version())
        self.assertEqual(btc_block.magic(), sent_btc_block.magic())
        self.assertEqual(btc_block.prev_block(), sent_btc_block.prev_block())
        self.assertEqual(btc_block.merkle_root(), sent_btc_block.merkle_root())
        self.assertEqual(btc_block.timestamp(), sent_btc_block.timestamp())
        self.assertEqual(btc_block.bits(), sent_btc_block.bits())
        self.assertEqual(btc_block.nonce(), sent_btc_block.nonce())
        self.assertEqual(btc_block.txn_count(), sent_btc_block.txn_count())

    def test_msg_key_wait_for_broadcast(self):
        btc_block, message = self.block()
        self.gateway_node.send_bytes_to_node = MagicMock()

        key, ciphertext = symmetric_encrypt(message)
        block_hash = crypto.double_sha256(ciphertext)

        self.gateway_node.send_bytes_to_node.assert_not_called()

        key_message = KeyMessage(ObjectHash(block_hash), key)
        self.sut.msg_key(key_message)

        # handle duplicate broadcasts
        self.sut.msg_key(key_message)
        self.sut.msg_key(key_message)

        self.assertEqual(1, len(self.gateway_node.in_progress_blocks))

        broadcast_message = BroadcastMessage(ObjectHash(block_hash), ciphertext)
        self.sut.msg_broadcast(broadcast_message)

        self.gateway_node.send_bytes_to_node.assert_called_once()
        ((sent_msg,), _) = self.gateway_node.send_bytes_to_node.call_args
        sent_btc_block = BlockBTCMessage(buf=bytearray(sent_msg))

        self.assertEqual(btc_block.version(), sent_btc_block.version())
        self.assertEqual(btc_block.magic(), sent_btc_block.magic())
        self.assertEqual(btc_block.prev_block(), sent_btc_block.prev_block())
        self.assertEqual(btc_block.merkle_root(), sent_btc_block.merkle_root())
        self.assertEqual(btc_block.timestamp(), sent_btc_block.timestamp())
        self.assertEqual(btc_block.bits(), sent_btc_block.bits())
        self.assertEqual(btc_block.nonce(), sent_btc_block.nonce())
        self.assertEqual(btc_block.txn_count(), sent_btc_block.txn_count())
