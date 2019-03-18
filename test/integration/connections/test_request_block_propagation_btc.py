import time

from mock import MagicMock

from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxcommon.test_utils import helpers
from bxcommon.utils.buffers.output_buffer import OutputBuffer
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway import gateway_constants
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxgateway.messages.gateway.block_propagation_request import BlockPropagationRequestMessage
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.testing.abstract_btc_gateway_integration_test import AbstractBtcGatewayIntegrationTest
from bxgateway.testing.mocks.mock_btc_messages import btc_block


class RequestBlockPropagationBtcTest(AbstractBtcGatewayIntegrationTest):

    def test_request_block_propagation(self):
        block = btc_block().rawbytes()

        # propagate block
        helpers.receive_node_message(self.node1, self.blockchain_fileno, block)

        block_hold_request_relay = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BlockHoldingMessage.MESSAGE_TYPE, block_hold_request_relay.tobytes())
        self.node1.on_bytes_sent(self.relay_fileno, len(block_hold_request_relay))

        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BroadcastMessage.MESSAGE_TYPE, relayed_block.tobytes())

        block_hold_request_gateway = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(BlockHoldingMessage.MESSAGE_TYPE, block_hold_request_gateway.tobytes())
        self.clear_all_buffers()

        # receipt timeout
        time.time = MagicMock(return_value=time.time() + gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S)
        self.node1.alarm_queue.fire_alarms()

        key_msg_gateway = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(KeyMessage.MESSAGE_TYPE, key_msg_gateway.tobytes())
        self.node1.on_bytes_sent(self.gateway_fileno, len(key_msg_gateway))

        block_prop_request = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(BlockPropagationRequestMessage.MESSAGE_TYPE, block_prop_request.tobytes())
        self.clear_all_buffers()

        # get new block to send
        helpers.receive_node_message(self.node2, self.gateway_fileno, block_prop_request)
        new_relayed_block = self.node2.get_bytes_to_send(self.relay_fileno)
        self.assertIn(BroadcastMessage.MESSAGE_TYPE, new_relayed_block.tobytes())
        helpers.clear_node_buffer(self.node2, self.relay_fileno)

        # receive new block
        helpers.receive_node_message(self.node1, self.relay_fileno, new_relayed_block)
        block_receipt = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIn(BlockReceivedMessage.MESSAGE_TYPE, block_receipt.tobytes())

        # receive block receipt
        helpers.receive_node_message(self.node2, self.gateway_fileno, block_receipt)
        key_message = self.node2.get_bytes_to_send(self.relay_fileno)
        self.assertIn(KeyMessage.MESSAGE_TYPE, key_message.tobytes())

        # receive key, but already seen so dont forward to blockchain
        helpers.receive_node_message(self.node1, self.relay_fileno, key_message)
        bytes_to_blockchain = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(OutputBuffer.EMPTY, bytes_to_blockchain)

        # clear blocks seen, rereceive
        self.node1.blocks_seen = ExpiringSet(self.node1.alarm_queue,
                                             gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)
        helpers.receive_node_message(self.node1, self.relay_fileno, key_message)
        # ignore key message even if block is not in "blocks_seen"
        bytes_to_blockchain = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(OutputBuffer.EMPTY, bytes_to_blockchain)

