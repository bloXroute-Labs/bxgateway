from bxgateway.block_hash_message import BlockHashMessage
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class BlockHoldingMessage(BlockHashMessage):
    """
    Request for other gateways to hold onto the block for a timeout to avoid encrypted block duplication.
    """
    MESSAGE_TYPE = GatewayMessageType.BLOCK_HOLDING

    def __repr__(self):
        return "BlockHoldingMessage<block_hash: {}>".format(self.block_hash())
