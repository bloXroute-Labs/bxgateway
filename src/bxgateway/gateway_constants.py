from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType

GATEWAY_HELLO_MESSAGES = [GatewayMessageType.HELLO, BloxrouteMessageType.ACK]

GATEWAY_BLOCKS_SEEN_EXPIRATION_TIME_S = 30 * 60

# Delay for blockchain sync message request before broadcasting to everyone
# This constants is currently unused
BLOCKCHAIN_SYNC_BROADCAST_DELAY_S = 5
BLOCKCHAIN_PING_INTERVAL_S = 2

BLOCK_RECOVERY_RECOVERY_INTERVAL_S = [0.1, 0.5, 1, 2, 5]
BLOCK_RECOVERY_MAX_RETRY_ATTEMPTS = len(BLOCK_RECOVERY_RECOVERY_INTERVAL_S)


# enum for setting Gateway neutrality assertion policy for releasing encryption keys
class NeutralityPolicy(object):
    RECEIPT_COUNT = 1
    RECEIPT_PERCENT = 2
    RECEIPT_COUNT_AND_PERCENT = 3

    RELEASE_IMMEDIATELY = 99


# duration to wait for block receipts until timeout
NEUTRALITY_BROADCAST_BLOCK_TIMEOUT_S = 30 * 60

NEUTRALITY_POLICY = NeutralityPolicy.RECEIPT_PERCENT
NEUTRALITY_EXPECTED_RECEIPT_COUNT = 1
NEUTRALITY_EXPECTED_RECEIPT_PERCENT = 50

MIN_INTERVAL_BETWEEN_BLOCKS_S = 1
NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S = 5

GATEWAY_TRANSACTION_STATS_INTERVAL = 60
GATEWAY_TRANSACTION_STATS_LOOKBACK = 1

# duration to hold block for if hold exists
BLOCK_HOLDING_TIMEOUT_S = 3 * 60
# duration hold should exist for
BLOCK_HOLD_DURATION_S = 3 * 60

MIN_PEER_RELAYS = 1

FASTEST_PING_LATENCY_THRESHOLD_PERCENT = 0.1

PING_TIMEOUT_S = 2

BLOCKCHAIN_SOCKET_SEND_BUFFER_SIZE = 16 * 1024 * 1024
