import time

from mock import MagicMock

from bxcommon.test_utils import helpers
from bxcommon.utils.buffers.output_buffer import OutputBuffer
from bxcommon.utils.expiring_set import ExpiringSet
from bxgateway import gateway_constants
from bxgateway.testing.abstract_btc_gateway_integration_test import AbstractBtcGatewayIntegrationTest
from bxgateway.testing.mocks.mock_btc_messages import btc_block


class RequestBlockPropagationBtcTest(AbstractBtcGatewayIntegrationTest):

    def test_request_block_propagation(self):
        block = btc_block().rawbytes()

        # propagate block
        helpers.receive_node_message(self.node1, self.blockchain_fileno, block)
        relayed_block = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertIsNotNone(relayed_block)

        # receipt timeout
        time.time = MagicMock(return_value=time.time() + gateway_constants.NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S)
        self.node1.alarm_queue.fire_alarms()
        block_prop_request = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIsNotNone(block_prop_request)
        self.clear_all_buffers()

        # get new block to send
        helpers.receive_node_message(self.node2, self.gateway_fileno, block_prop_request)
        new_relayed_block = self.node2.get_bytes_to_send(self.relay_fileno)
        self.assertIsNotNone(new_relayed_block)
        helpers.clear_node_buffer(self.node2, self.relay_fileno)

        # receive new block
        helpers.receive_node_message(self.node1, self.relay_fileno, new_relayed_block)
        block_receipt = self.node1.get_bytes_to_send(self.gateway_fileno)
        self.assertIsNotNone(self.gateway_fileno)

        # receive block receipt
        helpers.receive_node_message(self.node2, self.gateway_fileno, block_receipt)
        key_message = self.node2.get_bytes_to_send(self.relay_fileno)
        self.assertIsNotNone(key_message)

        # receive key, but already seen so dont forward to blockchain
        helpers.receive_node_message(self.node1, self.relay_fileno, key_message)
        bytes_to_blockchain = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(OutputBuffer.EMPTY, bytes_to_blockchain)

        # clear blocks seen, rereceive
        self.node1.blocks_seen = ExpiringSet(self.node1.alarm_queue,
                                             gateway_constants.GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S)
        helpers.receive_node_message(self.node1, self.relay_fileno, key_message)
        bytes_to_blockchain = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(len(block), len(bytes_to_blockchain))

