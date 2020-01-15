from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import (
    GetBlockBodiesEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import (
    GetBlockHeadersEthProtocolMessage,
)
from bxgateway.services.block_processing_service import BlockProcessingService
from bxutils import logging

logger = logging.get_logger(__name__)


class EthBlockProcessingService(BlockProcessingService):
    _node: "bxgateway.connections.eth.eth_gateway_node.EthGatewayNode"

    def try_process_get_block_headers_request(
            self, msg: GetBlockHeadersEthProtocolMessage
    ) -> bool:
        if msg.get_amount() == 1 or msg.get_skip() == 0:
            block_hash = msg.get_block_hash()
            if block_hash is not None:
                logger.trace("Checking for headers in local block cache...")
                return self._node.block_queuing_service.try_send_header_to_node(
                    block_hash
                )

        return False

    def try_process_get_block_bodies_request(
            self, msg: GetBlockBodiesEthProtocolMessage
    ) -> bool:
        block_hashes = msg.get_block_hashes()

        if len(block_hashes) == 1:
            block_hash = Sha256Hash(block_hashes[0])

            logger.trace("Checking for bodies in local block cache...")

            return self._node.block_queuing_service.try_send_body_to_node(
                block_hash
            )

        return False
