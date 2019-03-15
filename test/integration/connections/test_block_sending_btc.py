import time

from mock import patch, MagicMock

from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.test_utils import helpers
from bxcommon.utils import convert, crypto
from bxcommon.utils.buffers.output_buffer import OutputBuffer
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.btc_constants import BTC_SHA_HASH_LEN
from bxgateway.gateway_constants import NeutralityPolicy
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.gateway.block_holding_message import BlockHoldingMessage
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.testing.abstract_btc_gateway_integration_test import AbstractBtcGatewayIntegrationTest
from bxgateway.testing.mocks.mock_btc_messages import btc_block, RealBtcBlocks
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


@patch("bxcommon.constants.OUTPUT_BUFFER_MIN_SIZE", 0)
@patch("bxcommon.constants.OUTPUT_BUFFER_BATCH_MAX_HOLD_TIME", 0)
@patch("bxgateway.gateway_constants.NEUTRALITY_EXPECTED_RECEIPT_COUNT", 1)
@patch("bxgateway.gateway_constants.NEUTRALITY_POLICY", NeutralityPolicy.RECEIPT_COUNT)
class BlockSendingBtcTest(AbstractBtcGatewayIntegrationTest):
    def _populate_transaction_services(self, block):
        if len(block.txns()) > 0:
            first_transaction = block.txns()[0]
            first_transaction_hash = BtcObjectHash(crypto.bitcoin_hash(first_transaction), length=BTC_SHA_HASH_LEN)
            self.node1.get_tx_service().assign_short_id(first_transaction_hash, 1)
            self.node1.get_tx_service().set_transaction_contents(first_transaction_hash, first_transaction)
            self.node2.get_tx_service().assign_short_id(first_transaction_hash, 1)
            self.node2.get_tx_service().set_transaction_contents(first_transaction_hash, first_transaction)

    def send_received_block_and_key(self, block=None):
        if block is None:
            block = btc_block()

        self._populate_transaction_services(block)

        # propagate block
        helpers.receive_node_message(self.node1, self.blockchain_fileno, block.rawbytes())
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BroadcastMessage.MESSAGE_TYPE, relayed_block.tobytes())
        self.node1.on_bytes_sent(self.relay_fileno, len(relayed_block))

        # block hold sent out
        block_hold_request = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIsNotNone(block_hold_request)
        self.assertIn(BlockHoldingMessage.MESSAGE_TYPE, block_hold_request.tobytes())
        self.clear_all_buffers()

        # key not available until receipt
        key_not_yet_available = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(OutputBuffer.EMPTY, key_not_yet_available)

        # send receipt
        helpers.receive_node_message(self.node2, self.relay_fileno, relayed_block)
        block_receipt = self.node2.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(BlockReceivedMessage.MESSAGE_TYPE, block_receipt.tobytes())

        # send key
        helpers.receive_node_message(self.node1, self.gateway_fileno, block_receipt)
        key_message = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(KeyMessage.MESSAGE_TYPE, key_message.tobytes())

        helpers.receive_node_message(self.node2, self.relay_fileno, key_message)
        bytes_to_blockchain = self.node2.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(len(block.rawbytes()), len(bytes_to_blockchain))

        return BlockBtcMessage(buf=bytearray(bytes_to_blockchain))

    def test_send_receive_block_and_key_encrypted(self):
        send_block = btc_block(time.time())
        received_block = self.send_received_block_and_key(send_block)
        self.assertEqual(send_block.magic(), received_block.magic())
        self.assertEqual(send_block.version(), received_block.version())
        self.assertEqual(send_block.timestamp(), received_block.timestamp())
        self.assertEqual(send_block.bits(), received_block.bits())
        self.assertEqual(send_block.nonce(), received_block.nonce())

    def test_send_receive_block_decrypted(self):
        self.node1.opts.encrypt_blocks = False
        send_block = btc_block(time.time())
        self._populate_transaction_services(send_block)

        # propagate block
        helpers.receive_node_message(self.node1, self.blockchain_fileno, send_block.rawbytes())
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BroadcastMessage.MESSAGE_TYPE, relayed_block.tobytes())
        self.node1.on_bytes_sent(self.relay_fileno, len(relayed_block))

        # block directly propagated
        helpers.receive_node_message(self.node2, self.relay_fileno, relayed_block)
        bytes_to_blockchain = self.node2.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(len(send_block.rawbytes()), len(bytes_to_blockchain))

        received_block = BlockBtcMessage(buf=bytearray(bytes_to_blockchain))
        self.assertEqual(send_block.magic(), received_block.magic())
        self.assertEqual(send_block.version(), received_block.version())
        self.assertEqual(send_block.timestamp(), received_block.timestamp())
        self.assertEqual(send_block.bits(), received_block.bits())
        self.assertEqual(send_block.nonce(), received_block.nonce())

    def test_send_receive_block_and_key_real_block_1(self):
        self.node1.opts.blockchain_ignore_block_interval_count = 99999

        send_block = btc_block(real_block=RealBtcBlocks.BLOCK1)
        block_hash = "000000000000d76febe49ae1033fa22afebe6ac46ea255640268d7ede1084e6f"
        self.assertEqual(block_hash, convert.bytes_to_hex(send_block.block_hash().binary))

        received_block = self.send_received_block_and_key(block=send_block)
        self.assertEqual(9772, len(received_block.rawbytes()))
        self.assertEqual(536870912, received_block.version())
        self.assertEqual("bbb1f6f3a9324ab86ad23642994eac6624a9c9ef454d2f6e3bf68e3b094e0343",
                         convert.bytes_to_hex(received_block.merkle_root().binary))
        self.assertEqual(38, len(received_block.txns()))

        block_hash_object = ObjectHash(binary=convert.hex_to_bytes(block_hash))
        self.assertEqual(1, len(self.node1.blocks_seen.contents))
        self.assertIn(block_hash_object, self.node1.blocks_seen.contents)
        self.assertEqual(1, len(self.node2.blocks_seen.contents))
        self.assertIn(block_hash_object, self.node2.blocks_seen.contents)

    def test_send_receive_block_and_key_real_block_2(self):
        self.node1.opts.blockchain_ignore_block_interval_count = 99999

        received_block = self.send_received_block_and_key(
            block=btc_block(real_block=RealBtcBlocks.BLOCK_WITNESS_REJECT))
        self.assertEqual(9613, len(received_block.rawbytes()))
        self.assertEqual(536870912, received_block.version())
        self.assertEqual("66bbbabedebee6c045284299069c407cdaa493166e64fe92ae03f358e96c7164",
                         convert.bytes_to_hex(received_block.merkle_root().binary))
        self.assertEqual(47, len(received_block.txns()))

    def test_send_receive_block_and_key_block_hold_removed(self):
        block = btc_block()
        block_hash = block.block_hash()

        helpers.receive_node_message(self.node1, self.gateway_fileno, BlockHoldingMessage(block_hash).rawbytes())
        helpers.receive_node_message(self.node1, self.blockchain_fileno, block.rawbytes())
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(OutputBuffer.EMPTY, relayed_block)
        self.assertEqual(1, len(self.node1.block_processing_service._holds.contents))

        # node 2 encrypts and sends over block
        helpers.receive_node_message(self.node2, self.blockchain_fileno, block.rawbytes())
        relayed_block = self.node2.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BroadcastMessage.MESSAGE_TYPE, relayed_block.tobytes())
        self.node2.on_bytes_sent(self.relay_fileno, len(relayed_block))

        # send receipt
        helpers.receive_node_message(self.node1, self.relay_fileno, relayed_block)
        block_receipt = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(BlockReceivedMessage.MESSAGE_TYPE, block_receipt.tobytes())

        # send key
        helpers.receive_node_message(self.node2, self.gateway_fileno, block_receipt)
        key_message = self.node2.get_bytes_to_send(self.relay_fileno)
        self.assertIn(KeyMessage.MESSAGE_TYPE, key_message.tobytes())

        # receive key and lift hold
        helpers.receive_node_message(self.node1, self.relay_fileno, key_message)
        no_block_message = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(OutputBuffer.EMPTY, no_block_message)
        self.assertEqual(0, len(self.node1.block_processing_service._holds.contents))

    def test_block_hold_timeout(self):
        block = btc_block()
        block_hash = block.block_hash()

        helpers.receive_node_message(self.node1, self.gateway_fileno, BlockHoldingMessage(block_hash).rawbytes())
        helpers.receive_node_message(self.node1, self.blockchain_fileno, block.rawbytes())
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(OutputBuffer.EMPTY, relayed_block)
        self.assertEqual(1, len(self.node1.block_processing_service._holds.contents))

        time.time = MagicMock(
            return_value=time.time() + self.node1.block_processing_service._compute_hold_timeout(block))
        self.node1.alarm_queue.fire_alarms()

        # send ciphertext from node1, receipt from node2, key from node1
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BroadcastMessage.MESSAGE_TYPE, relayed_block.tobytes())
        self.node1.on_bytes_sent(self.relay_fileno, len(relayed_block))

        helpers.receive_node_message(self.node2, self.relay_fileno, relayed_block)
        block_receipt = self.node2.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(BlockReceivedMessage.MESSAGE_TYPE, block_receipt.tobytes())

        helpers.receive_node_message(self.node1, self.gateway_fileno, block_receipt)
        key_message = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(KeyMessage.MESSAGE_TYPE, key_message.tobytes())
        self.assertNotEqual(OutputBuffer.EMPTY, bytearray(key_message.tobytes()))

        helpers.receive_node_message(self.node2, self.relay_fileno, key_message)
        bytes_to_blockchain = self.node2.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(len(block.rawbytes()), len(bytes_to_blockchain))
