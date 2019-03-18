from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_factory import _BloxrouteMessageFactory
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.messages.bloxroute.key_message import KeyMessage
from bxgateway.messages.gateway.block_propagation_request import BlockPropagationRequestMessage
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.messages.gateway.blockchain_sync_request_message import BlockchainSyncRequestMessage
from bxgateway.messages.gateway.blockchain_sync_response_message import BlockchainSyncResponseMessage
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class _GatewayMessageFactory(_BloxrouteMessageFactory):
    _MESSAGE_TYPE_MAPPING = {
        GatewayMessageType.HELLO: GatewayHelloMessage,
        BloxrouteMessageType.ACK: AckMessage,
        GatewayMessageType.BLOCK_RECEIVED: BlockReceivedMessage,
        BloxrouteMessageType.BLOCK_HOLDING: BlockHoldingMessage,
        GatewayMessageType.BLOCK_PROPAGATION_REQUEST: BlockPropagationRequestMessage,
        BloxrouteMessageType.KEY: KeyMessage,

        # Sync messages are currently unused. See `blockchain_sync_service.py`
        GatewayMessageType.SYNC_REQUEST: BlockchainSyncRequestMessage,
        GatewayMessageType.SYNC_RESPONSE: BlockchainSyncResponseMessage
    }


gateway_message_factory = _GatewayMessageFactory()
