from typing import Union, cast, Optional

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.messages.eth.validation.eth_block_validator import EthBlockValidator
from bxcommon.utils.blockchain_utils.eth import rlp_utils
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import (
    GetBlockBodiesEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import (
    GetBlockHeadersEthProtocolMessage,
)
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxutils import logging

logger = logging.get_logger(__name__)


class EthBlockProcessingService(BlockProcessingService):
    _node: "bxgateway.connections.eth.eth_gateway_node.EthGatewayNode"

    def __init__(self, node):
        super(EthBlockProcessingService, self).__init__(node)

        self._block_validator = EthBlockValidator()

    def try_process_get_block_headers_request(
        self, msg: GetBlockHeadersEthProtocolMessage, block_queuing_service: Optional[EthBlockQueuingService]
    ) -> bool:
        if block_queuing_service is None:
            return False

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
        self, msg: GetBlockBodiesEthProtocolMessage, block_queuing_service: Optional[EthBlockQueuingService]
    ) -> bool:
        if block_queuing_service is None:
            return False

        block_hashes = msg.get_block_hashes()
        logger.trace("Checking for bodies in local block cache...")

        return block_queuing_service.try_send_bodies_to_node(
            block_hashes
        )

    def _get_compressed_block_header_bytes(self, compressed_block_bytes: Union[bytearray, memoryview]) -> Union[
        bytearray, memoryview]:
        block_msg_bytes = compressed_block_bytes if isinstance(compressed_block_bytes, memoryview) else memoryview(compressed_block_bytes)

        block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(block_msg_bytes)

        block_bytes = block_msg_bytes[block_offsets.block_begin_offset: block_offsets.short_id_offset]

        _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_bytes, 0)
        block_itm_bytes = block_bytes[block_itm_start:]

        _, block_hdr_len, block_hdr_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
        return block_itm_bytes[0:block_hdr_start + block_hdr_len]

    def _get_block_header_bytes_from_block_message(self, block_message: AbstractBlockMessage) -> Union[
        bytearray, memoryview]:
        eth_block_message = cast(InternalEthBlockInfo, block_message)
        return eth_block_message.block_header()
