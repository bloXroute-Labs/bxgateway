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

        block_queuing_service = self._node.block_queuing_service
        block_hash = msg.get_block_hash()

        if block_hash is not None:
            logger.trace(
                "Checking for headers by hash ({}) in local block cache...",
                block_hash,
            )
            (
                success, requested_block_hashes
            ) = block_queuing_service.get_block_hashes_starting_from_hash(
                block_hash,
                msg.get_amount(),
                msg.get_skip(),
                bool(msg.get_reverse()),
            )
        else:
            block_number = msg.get_block_number()
            if block_number:
                logger.trace(
                    "Checking for headers by block number ({}) "
                    "in local block cache",
                    block_number,
                )
                (
                    success, requested_block_hashes
                ) = block_queuing_service.get_block_hashes_starting_from_height(
                    block_number,
                    msg.get_amount(),
                    msg.get_skip(),
                    bool(msg.get_reverse()),
                )
            else:
                logger.debug(
                    "Unexpectedly, request for headers did not contain "
                    "block hash or block number. Skipping."
                )
                return False

        if success:
            return block_queuing_service.try_send_headers_to_node(
                requested_block_hashes
            )
        else:
            logger.trace(
                "Could not find requested block hashes. "
                "Forwarding to remote blockchain connection."
            )
            return False

    def try_process_get_block_bodies_request(
        self, msg: GetBlockBodiesEthProtocolMessage
    ) -> bool:
        block_hashes = msg.get_block_hashes()
        logger.trace("Checking for bodies in local block cache...")

        return self._node.block_queuing_service.try_send_bodies_to_node(
            block_hashes
        )
