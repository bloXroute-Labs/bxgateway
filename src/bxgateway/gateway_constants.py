from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType

GATEWAY_HELLO_MESSAGES = [GatewayMessageType.HELLO, BloxrouteMessageType.ACK]

GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S = 30 * 60

# Delay for blockchain sync message request before broadcasting to everyone
# This constants is currently unused
BLOCKCHAIN_SYNC_BROADCAST_DELAY_S = 5
BLOCKCHAIN_PING_INTERVAL_S = 2


# enum for setting Gateway neutrality assertion policy for releasing encryption keys
class NeutralityPolicy(object):
    RECEIPT_COUNT = 1
    RECEIPT_PERCENT = 2
    RECEIPT_COUNT_AND_PERCENT = 3

    RELEASE_IMMEDIATELY = 99


# duration to wait for block receipts until timeout
NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S = 30 * 60

NEUTRALITY_POLICY = NeutralityPolicy.RECEIPT_COUNT
NEUTRALITY_EXPECTED_RECEIPT_COUNT = 1
NEUTRALITY_EXPECTED_RECEIPT_PERCENT = 50
