from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType

GATEWAY_HELLO_MESSAGES = [GatewayMessageType.HELLO, BloxrouteMessageType.ACK]
GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S = 30 * 60

# Delay for blockchain sync message request before broadcasting to everyone
# This constants is currently unused
BLOCKCHAIN_SYNC_BROADCAST_DELAY_S = 5
BLOCKCHAIN_PING_INTERVAL_S = 2
