from bxutils.logging_messages_utils import LogMessage
from bxutils.log_message_categories import REQUEST_RESPONSE_CATEGORY, CONNECTION_PROBLEM_CATEGORY, NETWORK_CATEGORY, \
    GENERAL_CATEGORY, PROCESSING_FAILED_CATEGORY, LOGGING_CATEGORY, NOTIFICATION_FROM_RELAY_CATEGORY

NO_GATEWAY_PEERS = LogMessage(
    "G-000000",
    REQUEST_RESPONSE_CATEGORY,
    "Did not receive expected gateway peers from BDN. Retrying."
)
CLOSE_CONNECTION_ATTEMPT = LogMessage(
    "G-000001",
    CONNECTION_PROBLEM_CATEGORY,
    "Detected attempt to close node connection when another is already established. "
)
ABANDON_GATEWAY_PEER_REQUEST = LogMessage(
    "G-000002",
    NETWORK_CATEGORY,
    "Giving up on querying for gateway peers from the BDN."
)
TX_SYNC_TIMEOUT = LogMessage(
    "G-000003",
    CONNECTION_PROBLEM_CATEGORY,
    "Gateway transaction sync took too long; marking gateway as synced."
)
RELAY_CONNECTION_TIMEOUT = LogMessage(
    "G-000004",
    CONNECTION_PROBLEM_CATEGORY,
    "It has been more than {} seconds since the last time gateway received a message from requested "
)
BLOCKCHAIN_PEER_LACKS_PUBLIC_KEY = LogMessage(
    "G-000005",
    GENERAL_CATEGORY,
    "Received remote blockchain peer without remote public key. This is currently unsupported."
)
BLOCK_COUNT_MISMATCH = LogMessage(
    "G-000006",
    REQUEST_RESPONSE_CATEGORY,
    "Number of blocks received from remote blockchain node ({}) does not match expected ({}). "
    "Temporarily suspending logging of remote blocks sync."
)
INVALID_CONFIG_PAYLOAD = LogMessage(
    "G-000007",
    PROCESSING_FAILED_CATEGORY,
    "Payload in gateway config file ({}) was invalid."
)
READ_CONFIG_FAIL = LogMessage(
    "G-000008",
    PROCESSING_FAILED_CATEGORY,
    "Could not read contents of gateway config file ({}): {}"
)
UNEXPECTED_BLOCKS = LogMessage(
    "G-000009",
    NETWORK_CATEGORY,
    "Received blocks from remote node but nothing is expected."
)
INVALID_CONFIG_LOG_LEVEL = LogMessage(
    "G-000010",
    LOGGING_CATEGORY,
    "Invalid log level provided in config: {}."
)
BLOCK_RECOVERY_NEEDED = LogMessage(
    "G-000011",
    GENERAL_CATEGORY,
    "Block recovery needed. Missing {} sids, {} tx hashes. Total txs in bx_block: {}"
)
UNKNOWN_CALLBACK_MAPPING_WITH_SYNC_REQ = LogMessage(
    "G-000012",
    NETWORK_CATEGORY,
    "Received blockchain sync request type with unknown callback mapping: {}."
)
UNSOLICITED_SYNC_RESPONSE = LogMessage(
    "G-000013",
    REQUEST_RESPONSE_CATEGORY,
    "Received blockchain sync response type that was not asked for or already processed: {}"
)
UNKNOWN_SHORT_IDS = LogMessage(
    "G-000014",
    GENERAL_CATEGORY,
    "Compact block was parsed with {} unknown short ids. Requesting unknown transactions."
)
COMPACT_BLOCK_RECOVERY_FAIL = LogMessage(
    "G-000015",
    PROCESSING_FAILED_CATEGORY,
    "Failed to recover compact block '{}' after receiving BLOCK_TRANSACTIONS message. "
    "Requesting full block."
)
COMPACT_BLOCK_PROCESSING_FAIL = LogMessage(
    "G-000016",
    PROCESSING_FAILED_CATEGORY,
    "Failed to process compact block '{}' after recovery. Requesting full block. Error: {}"
)
STATUS_FILE_JSON_LOAD_FAIL = LogMessage(
    "G-000017",
    GENERAL_CATEGORY,
    "The status file may have been modified. This message can be safely ignored when "
    "running multiple nodes on the same machine. "
    "For more information, please check the status file: {}"
)
REDUNDANT_BLOCK_BODY = LogMessage(
    "G-000018",
    GENERAL_CATEGORY,
    "Block body for hash {} is not in the list of pending new blocks and not marked for cleanup."
)
NO_ACTIVE_BDN_CONNECTIONS = LogMessage(
    "G-000019",
    CONNECTION_PROBLEM_CATEGORY,
    "Gateway does not have an active connection to the relay network. "
    "There may be issues with the BDN. Exiting."
)
UNEXPECTED_TXS_ON_NON_RELAY_CONN = LogMessage(
    "G-000020",
    NETWORK_CATEGORY,
    "Received unexpected txs message on non-tx relay connection: {}"
)
UNEXPECTED_BLOCK_ON_NON_RELAY_CONN = LogMessage(
    "G-000021",
    NETWORK_CATEGORY,
    "Received unexpected block message on non-block relay connection: {}"
)
RPC_INITIALIZATION_FAIL = LogMessage(
    "G-000022",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to initialize Gateway RPC server: {}."
)
RPC_CLOSE_FAIL = LogMessage(
    "G-000023",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to close Gateway RPC server: {}."
)
INVALID_ACCOUNT_ID = LogMessage(
    "G-000024",
    GENERAL_CATEGORY,
    "Valid Account Id is required in order to propagate Transactions as Paid, revert quota type to Free"
)
NO_ACTIVE_BLOCKCHAIN_CONNECTION = LogMessage(
    "G-000025",
    CONNECTION_PROBLEM_CATEGORY,
    "Gateway does not have an active connection to the blockchain node. "
    "Check that the blockchain node is running and available. Exiting."
)
SHORT_ID_RECOVERY_FAIL = LogMessage(
    "G-000026",
    PROCESSING_FAILED_CATEGORY,
    "Could not decompress block {} after attempts to recover short ids. Discarding."
)
PAYLOAD_LENGTH_MISMATCH = LogMessage(
    "G-000027",
    GENERAL_CATEGORY,
    "Payload length does not match buffer size: {} vs {} bytes"
)
PACKET_CHECKSUM_MISMATCH = LogMessage(
    "G-000028",
    GENERAL_CATEGORY,
    "Checksum ({}) for packet doesn't match ({}): {}"
)
BLOCK_COMPRESSION_FAIL = LogMessage(
    "G-000029",
    PROCESSING_FAILED_CATEGORY,
    "Failed to compress block {} - {}"
)
MALFORMED_TX_FROM_RELAY = LogMessage(
    "G-000030",
    NETWORK_CATEGORY,
    "Received malformed transaction message from the BDN."
    "Expected hash: {}. Actual: {}"
)
SOCKET_BUFFER_SEND_FAIL = LogMessage(
    "G-000031",
    NETWORK_CATEGORY,
    "Could not set socket send buffer size: {}"
)
PONG_MESSAGE_TIMEOUT = LogMessage(
    "G-000032",
    CONNECTION_PROBLEM_CATEGORY,
    "Pong message was not received within allocated timeout connection. Closing."
)
BLOCK_REQUIRES_RECOVERY = LogMessage(
    "G-000033",
    GENERAL_CATEGORY,
    "Block {} requires short id recovery. Querying BDN..."
)
BLOCK_DECOMPRESSION_FAILURE = LogMessage(
    "G-000034",
    GENERAL_CATEGORY,
    "Unexpectedly, could not decompress block {} after block was recovered."
)
BLOCK_WITH_INCONSISTENT_HASHES = LogMessage(
    "G-000035",
    NETWORK_CATEGORY,
    "Received a block with inconsistent hashes from the BDN. "
    "Expected: {}. Actual: {}. Dropping."
)
LACK_BLOCKCHAIN_CONNECTION = LogMessage(
    "G-000036",
    CONNECTION_PROBLEM_CATEGORY,
    "Discarding block. No connection currently exists to the blockchain node."
)
MISMATCH_IP_IN_HELLO_MSG = LogMessage(
    "G-000037",
    REQUEST_RESPONSE_CATEGORY,
    "Received hello message with mismatched IP address. Continuing. Expected ip: {} Socket ip: {}"
)
FAILED_TO_DECOMPRESS_BLOCK = LogMessage(
    "G-000038",
    GENERAL_CATEGORY,
    "Failed to decompress block {} - {}"
)
REDUNDANT_CONNECTION = LogMessage(
    "G-000039",
    CONNECTION_PROBLEM_CATEGORY,
    "Connection already exists, with lower priority. Dropping connection {} and keeping {}."
)
HELLO_MSG_WITH_NO_PEER_ID = LogMessage(
    "G-000040",
    NETWORK_CATEGORY,
    "Received hello message without peer id."
)
PROCESS_BLOCK_FAILURE = LogMessage(
    "G-000041",
    PROCESSING_FAILED_CATEGORY,
    "Failed to process compact block: {}. Request full block. Error: {}"
)
NETWORK_NUMBER_MISMATCH = LogMessage(
    "G-000042",
    PROCESSING_FAILED_CATEGORY,
    "Network number mismatch. Current network num {}, remote network num {}. Closing connection."
)
UNEXPECTED_KEY_MESSAGE = LogMessage(
    "G-000043",
    PROCESSING_FAILED_CATEGORY,
    "Received unexpected key message on non-block relay connection: {}"
)
UNEXPECTED_TX_MESSAGE = LogMessage(
    "G-000044",
    PROCESSING_FAILED_CATEGORY,
    "Received unexpected tx message on non-tx relay connection: {}"
)
SET_SOCKET_BUFFER_SIZE = LogMessage(
    "G-000045",
    NETWORK_CATEGORY,
    "Socket buffer size set was unsuccessful, and was instead set to {}. Reverting to: {}"
)
NOTIFICATION_FROM_RELAY = LogMessage(
    "G-000046",
    NOTIFICATION_FROM_RELAY_CATEGORY,
    "Notification from Relay: {}"
)
