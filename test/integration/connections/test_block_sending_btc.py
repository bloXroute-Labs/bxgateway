from mock import patch

from bxcommon.constants import LOCALHOST
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.test_utils import helpers
from bxcommon.test_utils.helpers import get_gateway_opts
from bxcommon.utils import convert
from bxgateway.gateway_constants import NeutralityPolicy
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.testing.abstract_btc_gateway_integration_test import AbstractBtcGatewayIntegrationTest
from bxgateway.testing.mocks.mock_btc_messages import btc_block, RealBtcBlocks
from bxgateway.testing.unencrypted_block_cache import UnencryptedCache


@patch("bxcommon.constants.OUTPUT_BUFFER_MIN_SIZE", 0)
@patch("bxcommon.constants.OUTPUT_BUFFER_BATCH_MAX_HOLD_TIME", 0)
@patch("bxgateway.gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_COUNT", 1)
@patch("bxgateway.gateway_constants.NEUTRALITY_POLICY", NeutralityPolicy.RECEIPT_COUNT)
class BlockSendingBtcTest(AbstractBtcGatewayIntegrationTest):
    def send_received_block_and_key(self, block=None):
        if block is None:
            block = btc_block()

        # propagate block
        helpers.receive_node_message(self.node1, self.blockchain_fileno, block.rawbytes())
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIsNotNone(relayed_block)
        self.node1.on_bytes_sent(self.relay_fileno, len(relayed_block))

        # key not available until receipt
        key_not_yet_available = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIsNone(key_not_yet_available)

        # send receipt
        helpers.receive_node_message(self.node2, self.relay_fileno, relayed_block)
        block_receipt = self.node2.get_bytes_to_send(self.gateway_fileno)
        self.assertIsNotNone(block_receipt)

        # send key
        helpers.receive_node_message(self.node1, self.gateway_fileno, block_receipt)
        key_message = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIsNotNone(key_message)

        helpers.receive_node_message(self.node2, self.relay_fileno, key_message)
        bytes_to_blockchain = self.node2.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(len(block.rawbytes()), len(bytes_to_blockchain))

        return BlockBtcMessage(buf=bytearray(bytes_to_blockchain))

    def test_send_receive_block_and_key_encrypted(self):
        received_block = self.send_received_block_and_key()
        self.assertEqual(12345, received_block.magic())
        self.assertEqual(23456, received_block.version())
        self.assertEqual(1, received_block.timestamp())
        self.assertEqual(2, received_block.bits())
        self.assertEqual(3, received_block.nonce())

    def test_send_received_block_and_key_no_encrypt(self):
        node1_opts = get_gateway_opts(9000, test_mode=["disable-encryption"],
                                      peer_gateways=[OutboundPeerModel(LOCALHOST, 7002)],
                                      include_default_btc_args=True)
        node2_opts = get_gateway_opts(9001, test_mode=["disable-encryption"],
                                      peer_gateways=[OutboundPeerModel(LOCALHOST, 7002)],
                                      include_default_btc_args=True)
        self.reinitialize_gateways(node1_opts, node2_opts)
        received_block = self.send_received_block_and_key()

        self.assertEqual(12345, received_block.magic())
        self.assertEqual(23456, received_block.version())
        self.assertEqual(1, received_block.timestamp())
        self.assertEqual(2, received_block.bits())
        self.assertEqual(3, received_block.nonce())

        relayed_key = KeyMessage(buf=bytearray(self.node1.get_bytes_to_send(2)))
        # hack, cannot initialize this field properly without a further messages refactor
        relayed_key._payload_len = None
        self.assertEqual(UnencryptedCache.NO_ENCRYPT_KEY, relayed_key.key().tobytes())

    def test_send_receive_block_and_key_real_blocks(self):
        block1 = self.send_received_block_and_key(block=btc_block(real_block=RealBtcBlocks.BLOCK1))
        self.assertEqual(9772, len(block1.rawbytes()))
        self.assertEqual(536870912, block1.version())
        self.assertEqual("bbb1f6f3a9324ab86ad23642994eac6624a9c9ef454d2f6e3bf68e3b094e0343",
                         convert.bytes_to_hex(block1.merkle_root().binary))
        self.assertEqual(38, len(block1.txns()))

