from bxcommon.messages.bloxroute.ack_message import AckMessage
from bxcommon.messages.bloxroute.bloxroute_message_factory import _BloxrouteMessageFactory
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxgateway.messages.gateway.blockchain_sync_request_message import BlockchainSyncRequestMessage
from bxgateway.messages.gateway.blockchain_sync_response_message import BlockchainSyncResponseMessage
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class _GatewayMessageFactory(_BloxrouteMessageFactory):
    _MESSAGE_TYPE_MAPPING = {
        GatewayMessageType.HELLO: GatewayHelloMessage,
        BloxrouteMessageType.ACK: AckMessage,

        # Sync messages are currently unused. See `blockchain_sync_service.py`
        GatewayMessageType.SYNC_REQUEST: BlockchainSyncRequestMessage,
        GatewayMessageType.SYNC_RESPONSE: BlockchainSyncResponseMessage
    }


gateway_message_factory = _GatewayMessageFactory()
