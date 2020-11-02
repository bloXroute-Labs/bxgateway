from bxutils.logging_messages_utils import LogMessage
from bxutils.log_message_categories import REQUEST_RESPONSE_CATEGORY, CONNECTION_PROBLEM_CATEGORY, \
    NETWORK_CATEGORY, MEMORY_CATEGORY, \
    GENERAL_CATEGORY, PROCESSING_FAILED_CATEGORY, LOGGING_CATEGORY, \
    NOTIFICATION_FROM_RELAY_CATEGORY, AUTHENTICATION_ERROR, RPC_ERROR

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
_BLOCK_REQUIRES_RECOVERY = LogMessage(
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
WS_INITIALIZATION_FAIL = LogMessage(
    "G-000047",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to initialize Gateway Websockets server: {}."
)
WS_CLOSE_FAIL = LogMessage(
    "G-000048",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to close Gateway Websockets server: {}."
)
ETH_WS_INITIALIZATION_FAIL = LogMessage(
    "G-000049",
    CONNECTION_PROBLEM_CATEGORY,
    "Could not connect to the Ethereum websockets server to verify transactions. "
    "Ensure that you have included the `--ws`, `--ws-api eth`, and `--ws-port PORT` "
    "arguments when starting your Ethereum node, and have specified the right "
    "websocket connection string for `--eth-ws-uri` on your gateway. "
    "If you would like the gateway to run without this functionality, you can remove "
    "the `--eth-ws-uri` argument. Error: {}"
)
ETH_WS_CLOSE_FAIL = LogMessage(
    "G-000050",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to close Etheruem websockets connection: {}."
)
BLOCK_RECOVERY_NEEDED_ONT_CONSENSUS = LogMessage(
    "G-000051",
    GENERAL_CATEGORY,
    "Consensus block recovery needed. Missing {} sids, {} tx hashes. Total txs in bx_block: {}"
)
BLOCK_COMPRESSION_FAIL_ONT_CONSENSUS = LogMessage(
    "G-000052",
    PROCESSING_FAILED_CATEGORY,
    "Failed to compress consensus block {} - {}"
)
_BLOCK_REQUIRES_RECOVERY_ONT_CONSENSUS = LogMessage(
    "G-000053",
    GENERAL_CATEGORY,
    "Consensus block {} requires short id recovery. Querying BDN..."
)
BLOCK_DECOMPRESSION_FAILURE_ONT_CONSENSUS = LogMessage(
    "G-000054",
    GENERAL_CATEGORY,
    "Unexpectedly, could not decompress consensus block {} after block was recovered."
)
LACK_BLOCKCHAIN_CONNECTION_ONT_CONSENSUS = LogMessage(
    "G-000055",
    CONNECTION_PROBLEM_CATEGORY,
    "Discarding consensus block. No connection currently exists to the blockchain node."
)
FAILED_TO_DECOMPRESS_BLOCK_ONT_CONSENSUS = LogMessage(
    "G-000056",
    GENERAL_CATEGORY,
    "Failed to decompress consensus block {} - {}"
)
IPC_INITIALIZATION_FAIL = LogMessage(
    "G-000057",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to initialize Gateway IPC server: {}."
)
IPC_CLOSE_FAIL = LogMessage(
    "G-000058",
    CONNECTION_PROBLEM_CATEGORY,
    "Failed to close Gateway IPC server: {}."
)
TRACKED_BLOCK_CLEANUP_ERROR = LogMessage(
    "G-000059",
    GENERAL_CATEGORY,
    "tracked block cleanup failed, {}"
)
COULD_NOT_DESERIALIZE_TRANSACTION = LogMessage(
    "G-000060",
    PROCESSING_FAILED_CATEGORY,
    "Could not deserialize transaction in transaction service to Ethereum payload: {}, body: {}. Error: {}",
)
TRANSACTION_NOT_FOUND_IN_MEMPOOL = LogMessage(
    "G-000061",
    GENERAL_CATEGORY,
    "Transaction was not found in mempool, despite publication to pendingTxs feed. Publishing "
    "empty contents for {}"
)
ETH_RPC_PROCESSING_ERROR = LogMessage(
    "G-000062",
    PROCESSING_FAILED_CATEGORY,
    "Encountered exception when processing message: {}. Error: {}. Continuing processing.",
)
ETH_RPC_COULD_NOT_RECONNECT = LogMessage(
    "G-000063",
    CONNECTION_PROBLEM_CATEGORY,
    "Could not reconnect to Ethereum websockets feed. Disabling Ethereum transaction "
    "verification for now, but will attempt reconnection upon when the next subscriber "
    "reconnects."
)
WS_COULD_NOT_CONNECT = LogMessage(
    "G-000064",
    CONNECTION_PROBLEM_CATEGORY,
    "Could not connect to websockets server at {}. Connection timed out.",
)
ETH_RPC_ERROR = LogMessage(
    "G-000065",
    PROCESSING_FAILED_CATEGORY,
    "RPC Error response: {}. details: {}.",
)
TRANSACTION_FEED_NOT_ALLOWED = LogMessage(
    "G-000066",
    AUTHENTICATION_ERROR,
    "Account does not have permission to stream transactions. Websocket server will be started "
    "normally, but live transactions will not be available for subscription."
)
ETH_WS_SUBSCRIBER_NOT_STARTED = LogMessage(
    "G-000067",
    GENERAL_CATEGORY,
    "Local Ethereum transaction validation is not enabled. Websocket server will be started "
    "normally, but transaction feed validation will only comes from the BDN and not your connected "
    "Ethereum node, which may provide better performance. Considering starting your gateway with "
    "--eth-ws-uri ws://[ip_address]:[port] for faster transaction streaming."
)
GATEWAY_BAD_FEED_SUBSCRIBER = LogMessage(
    "G-000068",
    GENERAL_CATEGORY,
    "Subscriber {} was not receiving messages and emptying its queue from "
    "{}. Disconnecting.",
)
_BAD_RPC_SUBSCRIBER = LogMessage(
    "G-000069",
    RPC_ERROR,
    "Subscription message queue was completed filled up (size {}). "
    "Closing subscription RPC handler and all related subscriptions: {}"
)
ETH_WS_SUBSCRIBER_CONNECTION_BROKEN = LogMessage(
    "G-000070",
    RPC_ERROR,
    "Ethereum websockets connection was broken. Attempting reconnection..."
)
ETH_MISSING_BLOCKCHAIN_IP_AND_BLOCKCHAIN_PEERS = LogMessage(
    "G-000071",
    GENERAL_CATEGORY,
    "At least one of the following four arguments is required for blockchain node connection:"
    "--blockchain-ip, --enode, --blockchain-peers, or --blockchain-peers-file"
)
ETH_MISSING_NODE_PUBLIC_KEY = LogMessage(
    "G-000072",
    GENERAL_CATEGORY,
    "--node-public-key argument is required but not specified."
)
MISSING_BLOCKCHAIN_IP_AND_BLOCKCHAIN_PEERS = LogMessage(
    "G-000073",
    GENERAL_CATEGORY,
    "At least one of the following three arguments is required for blockchain node connection:"
    "--blockchain-ip, --blockchain-peers, or --blockchain-peers-file"
)
MISSING_BLOCKCHAIN_PROTOCOL = LogMessage(
    "G-000074",
    GENERAL_CATEGORY,
    "Blockchain protocol information is missing exiting."
)
INVALID_PUBLIC_KEY_LENGTH = LogMessage(
    "G-000075",
    GENERAL_CATEGORY,
    "Public key must be the 128 digit key associated with the blockchain enode. "
    "Invalid key length: {}"
)
INVALID_PUBLIC_KEY = LogMessage(
    "G-000076",
    GENERAL_CATEGORY,
    "Public key must be constructed from a valid private key."
)
INVALID_BLOCKCHAIN_IP = LogMessage(
    "G-000077",
    GENERAL_CATEGORY,
    "The specified blockchain IP is localhost, which is not compatible with a dockerized "
    "gateway. Did you mean 172.17.0.X?"
)
BLOCKCHAIN_IP_RESOLVE_ERROR = LogMessage(
    "G-000078",
    GENERAL_CATEGORY,
    "Blockchain IP could not be resolved, exiting. Blockchain IP: {}"
)
_COULD_NOT_SERIALIZE_FEED_ENTRY = LogMessage(
    "G-000079",
    PROCESSING_FAILED_CATEGORY,
    "Could not serialize feed entry. Skipping."
)
MISSING_TRANSACTION_STREAMER_PEER_INFO = LogMessage(
    "G-000080",
    CONNECTION_PROBLEM_CATEGORY,
    "Missing transaction streamer peer info upon feed subscription. Connect attempt aborted. Please retry subscription if "
    "you wish to subscribe to the pendingTxs feed and local Ethereum validation is not enabled via --eth-ws-uri."
)
COULD_NOT_DESERIALIZE_BLOCK = LogMessage(
    "G-000081",
    PROCESSING_FAILED_CATEGORY,
    "Could not deserialize block to Ethereum payload: {}, body: {}. Error: {}",
)
BLOCKCHAIN_PROTOCOL_AND_ACCOUNT_MISMATCH = LogMessage(
    "G-000082",
    GENERAL_CATEGORY,
    "Blockchain protocol information does not match account details, exiting."
)
ETH_MISSING_REMOTE_NODE_PUBLIC_KEY = LogMessage(
    "G-000083",
    GENERAL_CATEGORY,
    "--remote-public-key of the blockchain node must be included with command-line specified remote blockchain peer. "
    "Use --remote-public-key."
)
MISSING_BLOCKCHAIN_IP_FROM_BLOCKCHAIN_PEERS = LogMessage(
    "G-000084",
    GENERAL_CATEGORY,
    "blockchain ip is required but not specified in --blockchain-peers argument"
)
ETH_MISSING_BLOCKCHAIN_IP_FROM_BLOCKCHAIN_PEERS = LogMessage(
    "G-000085",
    GENERAL_CATEGORY,
    "Blockchain ip is required but not specified in --blockchain-peers or --blockchain-peers-file"
)
ETH_MISSING_NODE_PUBLIC_KEY_FROM_BLOCKCHAIN_PEERS = LogMessage(
    "G-000086",
    GENERAL_CATEGORY,
    "Node public key is required but not specified in --blockchain-peers or --blockchain-peers-file"
)
ETH_PARSER_INVALID_ENODE_LENGTH = LogMessage(
    "G-000087",
    GENERAL_CATEGORY,
    "Invalid enode: {}, with length: {}"
)
ETH_PARSER_INVALID_ENODE = LogMessage(
    "G-000088",
    GENERAL_CATEGORY,
    "Invalid enode: {}"
)
PARSER_INVALID_PORT = LogMessage(
    "G-000089",
    GENERAL_CATEGORY,
    "Invalid port: {}"
)
BLOCKCHAIN_PEERS_FILE_NOT_FOUND = LogMessage(
    "G-000090",
    GENERAL_CATEGORY,
    "Blockchain peers file not found at path: {}"
)
NODE_EXCEEDS_MEMORY = LogMessage(
    "G-000091",
    MEMORY_CATEGORY,
    "Gateway exceeded allowed memory, restarting"
)
ATTEMPTED_FETCH_FOR_NONEXISTENT_QUEUING_SERVICE = LogMessage(
    "G-000092",
    GENERAL_CATEGORY,
    "Attmepted to fetch queuing service for blockchain node {}, but it was not found."
)
