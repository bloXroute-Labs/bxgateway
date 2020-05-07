from mock import Mock, MagicMock

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import \
    BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import \
    BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import \
    GetBlockBodiesEthProtocolMessage
from bxgateway.services.eth.eth_block_processing_service import EthBlockProcessingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class EthBlockProcessingServiceTest(AbstractTestCase):
    def setUp(self) -> None:
        self.node = MockGatewayNode(
            helpers.get_gateway_opts(8000, max_block_interval=0)
        )

        self.node.send_msg_to_node = MagicMock()

        self.block_queuing_service = EthBlockQueuingService(self.node)
        self.node.block_queuing_service = self.block_queuing_service

        self.block_hashes = []
        self.block_messages = []
        self.block_headers = []
        self.block_bodies = []

        # block numbers: 1000-1019
        prev_block_hash = None
        for i in range(20):
            block_message = InternalEthBlockInfo.from_new_block_msg(
                mock_eth_messages.new_block_eth_protocol_message(
                    i, i + 1000, prev_block_hash=prev_block_hash
                )
            )
            block_hash = block_message.block_hash()
            self.block_hashes.append(block_hash)
            self.block_messages.append(block_message)

            block_parts = block_message.to_new_block_parts()
            self.block_headers.append(
                BlockHeadersEthProtocolMessage.from_header_bytes(
                    block_parts.block_header_bytes
                ).get_block_headers()[0]
            )
            self.block_bodies.append(
                BlockBodiesEthProtocolMessage.from_body_bytes(
                    block_parts.block_body_bytes
                ).get_blocks()[0]
            )

            self.block_queuing_service.push(block_hash, block_message)
            prev_block_hash = block_hash

        self.block_processing_service = EthBlockProcessingService(self.node)
        self.node.send_msg_to_node.reset_mock()

    def test_try_process_get_block_bodies_request(self):
        success = self.block_processing_service.try_process_get_block_bodies_request(
            GetBlockBodiesEthProtocolMessage(
                None,
                [block_hash.binary for block_hash in self.block_hashes[:2]]
            )
        )
        self.assertTrue(success)
        self.node.send_msg_to_node.assert_called_once_with(
            BlockBodiesEthProtocolMessage(
                None,
                self.block_bodies[:2]
            )
        )

    def test_try_process_get_block_bodies_request_not_found(self):
        success = self.block_processing_service.try_process_get_block_bodies_request(
            GetBlockBodiesEthProtocolMessage(
                None,
                [self.block_hashes[0].binary, bytes(helpers.generate_hash())]
            )
        )
        self.assertFalse(success)
        self.node.send_msg_to_node.assert_not_called()
