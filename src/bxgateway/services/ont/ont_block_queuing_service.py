from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage

from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.utils.ont.ont_object_hash import OntObjectHash


# TODO: Skeleton class for now, need to implement block queuing
class OntBlockQueuingService(BlockQueuingService[BlockOntMessage]):
    def get_previous_block_hash_from_message(self, block_message: BlockOntMessage) -> OntObjectHash:
        return block_message.prev_block_hash()

    def on_block_sent(self, block_hash: OntObjectHash, block_message: BlockOntMessage):
        inv_msg = InvOntMessage(magic=block_message.magic(), inv_type=InventoryOntType.MSG_BLOCK.value, blocks=[block_hash])
        self.node.send_msg_to_node(inv_msg)
