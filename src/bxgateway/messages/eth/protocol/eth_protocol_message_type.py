class EthProtocolMessageType:
    AUTH = b"auth"
    AUTH_ACK = b"auth_ack"
    HELLO = 0
    DISCONNECT = 1
    PING = 2
    PONG = 3
    STATUS = 16
    NEW_BLOCK_HASHES = 17
    TRANSACTIONS = 18
    GET_BLOCK_HEADERS = 19
    BLOCK_HEADERS = 20
    GET_BLOCK_BODIES = 21
    BLOCK_BODIES = 22
    NEW_BLOCK = 23
    GET_NODE_DATA = 29
    NODE_DATA = 30
    GET_RECEIPTS = 31
    RECEIPTS = 32

    NEW_BLOCK_BYTES = b"new_block"

