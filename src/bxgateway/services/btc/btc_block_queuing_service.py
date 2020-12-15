from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import (
    InvBtcMessage,
    InventoryType,
)
from bxgateway.services.push_block_queuing_service import PushBlockQueuingService


class BtcBlockQueuingService(
    PushBlockQueuingService[BlockBtcMessage, InvBtcMessage]
):
    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: BlockBtcMessage
    ) -> InvBtcMessage:
        return InvBtcMessage(
            block_message.magic(), [(InventoryType.MSG_BLOCK, block_hash)]
        )

    def get_previous_block_hash_from_message(
        self, block_message: BlockBtcMessage
    ) -> Sha256Hash:
        return block_message.prev_block_hash()

    def on_block_sent(
        self, block_hash: Sha256Hash, block_message: BlockBtcMessage
    ):
        # After sending block message to Bitcoin node sending INV message for the same block to the node
        # This is needed to update Synced Headers value of the gateway peer on the Bitcoin node
        # If Synced Headers is not up-to-date than Bitcoin node does not push compact blocks to the gateway
        inv_msg = InvBtcMessage(
            magic=block_message.magic(),
            inv_vects=[(InventoryType.MSG_BLOCK, block_hash)],
        )
        self.connection.enqueue_msg(inv_msg)