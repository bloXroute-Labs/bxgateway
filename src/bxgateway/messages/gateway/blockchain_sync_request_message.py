from bxgateway.messages.gateway.abstract_blockchain_sync_message import AbstractBlockchainSyncMessage
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class BlockchainSyncRequestMessage(AbstractBlockchainSyncMessage):
    """
    Message type for requesting direct blockchain messages for syncing chainstate.

    NOTE:
    This class is currently unused. See `blockchain_sync_service.py` for reasons.
    """
    MESSAGE_TYPE = GatewayMessageType.SYNC_REQUEST
