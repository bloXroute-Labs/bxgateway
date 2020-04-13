from typing import List, Optional, cast, TYPE_CHECKING

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import ont_constants
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.services.abstract_block_queuing_service import (
    AbstractBlockQueuingService,
)
from bxgateway.utils.ont.ont_object_hash import OntObjectHash
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode

logger = logging.get_logger(__name__)


class OntBlockQueuingService(
    AbstractBlockQueuingService[BlockOntMessage, HeadersOntMessage]
):
    """
    Blocks sent to blockchain node only upon request
    """
    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self.node: "OntGatewayNode" = cast("OntGatewayNode", node)

    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: BlockOntMessage
    ) -> HeadersOntMessage:
        return HeadersOntMessage(
            magic=block_message.magic(), headers=[block_message.header()]
        )

    def push(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[BlockOntMessage] = None,
        waiting_for_recovery: bool = False,
    ) -> None:
        super().push(block_hash, block_msg, waiting_for_recovery)
        logger.debug("Added block {} to queuing service", block_hash)
        self._clean_block_queue()

        if block_hash in self._blocks and not waiting_for_recovery:
            assert block_msg is not None
            block_hash = cast(OntObjectHash, block_hash)
            self.node.update_current_block_height(
                block_msg.height(), block_hash
            )
            inv_msg = InvOntMessage(
                magic=block_msg.magic(),
                inv_type=InventoryOntType.MSG_BLOCK,
                blocks=[block_hash],
            )
            self.node.send_msg_to_node(inv_msg)

    def send_block_to_node(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[BlockOntMessage] = None,
    ) -> None:
        if block_hash not in self._blocks:
            return

        waiting_for_recovery = self._blocks_waiting_for_recovery[block_hash]
        if waiting_for_recovery:
            raise Exception(
                "Block {} is waiting for recovery".format(block_hash)
            )

        block_msg = self._blocks[block_hash]
        assert block_msg is not None
        super(OntBlockQueuingService, self).send_block_to_node(
            block_hash, block_msg
        )

    def update_recovered_block(
        self, block_hash: OntObjectHash, block_msg: BlockOntMessage
    ):
        if block_hash not in self._blocks:
            return

        self._blocks_waiting_for_recovery[block_hash] = False
        self.store_block_data(block_hash, block_msg)
        self.node.update_current_block_height(block_msg.height(), block_hash)
        inv_msg = InvOntMessage(
            magic=block_msg.magic(),
            inv_type=InventoryOntType.MSG_BLOCK,
            blocks=[block_hash],
        )
        self.node.send_msg_to_node(inv_msg)

    def mark_blocks_seen_by_blockchain_node(
        self, block_hashes: List[OntObjectHash]
    ):
        for block_hash in block_hashes:
            self.mark_block_seen_by_blockchain_node(block_hash)

    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: OntObjectHash,
        block_message: Optional[BlockOntMessage] = None,
    ):
        self._blocks_seen_by_blockchain_node.add(block_hash)

    def _clean_block_queue(self):
        if len(self._block_queue) > ont_constants.ONT_MAX_QUEUED_BLOCKS:
            oldest_block_hash = self._block_queue[0].block_hash
            self.remove(oldest_block_hash)
            self.node.track_block_from_bdn_handling_ended(oldest_block_hash)
