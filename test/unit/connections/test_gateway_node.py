import socket

from mock import MagicMock

from bxcommon.constants import BTC_HDR_COMMON_OFF
from bxcommon.messages.bloxroute.hello_message import HelloMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.messages.btc.block_btc_message import BlockBTCMessage
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.messages.btc.version_btc_message import VersionBTCMessage
from bxcommon.network.socket_connection import SocketConnection
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import get_gateway_opts
from bxcommon.utils import crypto
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import BTCObjectHash
from bxgateway.connections.gateway_node import GatewayNode
from bxgateway.testing.unencrypted_block_cache import UnencryptedCache


def btc_block():
    magic = 12345
    version = 23456
    prev_block_hash = bytearray(crypto.double_sha256("123"))
    prev_block = BTCObjectHash(prev_block_hash, length=SHA256_HASH_LEN)
    merkle_root_hash = bytearray(crypto.double_sha256("234"))
    merkle_root = BTCObjectHash(merkle_root_hash, length=SHA256_HASH_LEN)
    timestamp = 1
    bits = 2
    nonce = 3

    txns = [TxBTCMessage(magic, version, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]

    return BlockBTCMessage(magic, version, prev_block, merkle_root, timestamp, bits, nonce, txns)


def clear_node_buffer(node, fileno):
    while node.get_bytes_to_send(fileno) is not None:
        node.on_bytes_sent(fileno, len(node.get_bytes_to_send(fileno)))


def receive_node_message(node, fileno, message):
    node.on_bytes_received(fileno, message)
    node.on_finished_receiving(fileno)


class GatewayNodeTests(AbstractTestCase):

    def send_received_block_and_key(self, node1, node2):
        blockchain_fileno = 1
        blockchain_connection = SocketConnection(MagicMock(spec=socket.socket), None)
        blockchain_connection.fileno = MagicMock(return_value=blockchain_fileno)
        relay_fileno = 2
        relay_connection = SocketConnection(MagicMock(spec=socket.socket), None)
        relay_connection.fileno = MagicMock(return_value=relay_fileno)

        node1.on_connection_added(blockchain_connection, "127.0.0.1", 7000, False)
        receive_node_message(node1, blockchain_fileno, VersionBTCMessage(12345, 12345, "0.0.0.0", 1000, "0.0.0.0",
                                                                         1000, 1, 2, "bloxroute").rawbytes())

        node1.on_connection_added(relay_connection, "127.0.0.1", 7001, False)
        receive_node_message(node1, relay_fileno, HelloMessage(1).rawbytes())

        node2.on_connection_added(relay_connection, "127.0.0.1", 7001, False)
        receive_node_message(node2, relay_fileno, HelloMessage(1).rawbytes())

        node2.on_connection_added(blockchain_connection, "127.0.0.1", 7000, False)
        receive_node_message(node2, blockchain_fileno, VersionBTCMessage(12345, 12345, "0.0.0.0", 1000, "0.0.0.0",
                                                                         1000, 1, 2, "bloxroute").rawbytes())

        clear_node_buffer(node1, blockchain_fileno)
        clear_node_buffer(node1, relay_fileno)
        clear_node_buffer(node2, blockchain_fileno)
        clear_node_buffer(node2, relay_fileno)

        block = btc_block()
        receive_node_message(node1, blockchain_fileno, block.rawbytes())

        relayed_block = node1.get_bytes_to_send(relay_fileno)
        node1.on_bytes_sent(relay_fileno, len(relayed_block))
        relayed_key = node1.get_bytes_to_send(relay_fileno)

        receive_node_message(node2, relay_fileno, relayed_block)
        receive_node_message(node2, relay_fileno, relayed_key)
        bytes_to_blockchain = node2.get_bytes_to_send(blockchain_fileno)
        self.assertEqual(len(block.rawbytes()), len(bytes_to_blockchain))

        received_block = BlockBTCMessage(buf=bytearray(bytes_to_blockchain))
        self.assertEqual(12345, received_block.magic())
        self.assertEqual(23456, received_block.version())
        self.assertEqual(1, received_block.timestamp())
        self.assertEqual(2, received_block.bits())
        self.assertEqual(3, received_block.nonce())

    def test_send_receive_block_and_key_encrypted(self):
        node1 = GatewayNode(get_gateway_opts(9000))
        node2 = GatewayNode(get_gateway_opts(9001))
        self.send_received_block_and_key(node1, node2)

    def test_send_received_block_and_key_no_encrypt(self):
        node1_opts = get_gateway_opts(9000)
        node1_opts.test_mode = ["disable-encryption"]
        node1 = GatewayNode(node1_opts)
        node2_opts = get_gateway_opts(9001)
        node2_opts.test_mode = ["disable-encryption"]
        node2 = GatewayNode(node2_opts)

        self.send_received_block_and_key(node1, node2)

        relayed_key = KeyMessage(buf=bytearray(node1.get_bytes_to_send(2)))
        # hack, cannot initialize this field properly without a further messages refactor
        relayed_key._payload_len = None
        self.assertEqual(UnencryptedCache.NO_ENCRYPT_KEY, relayed_key.key().tobytes())
