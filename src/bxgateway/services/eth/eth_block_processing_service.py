from typing import List

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

        block_hash = msg.get_block_hash()
        if block_hash is not None:
            logger.trace(
                "Checking for headers by hash ({}) in local block cache...",
                block_hash,
            )
            requested_block_hashes = self._node.block_queuing_service.get_block_hashes_starting_from_hash(
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
                requested_block_hashes = self._node.block_queuing_service.get_block_hashes_starting_from_height(
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

        if not requested_block_hashes:
            logger.trace(
                "Could not find requested block hashes. "
                "Forwarding to remote blockchain connection."
            )
            return False

        return self._node.block_queuing_service.try_send_headers_to_node(
            requested_block_hashes
        )

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
