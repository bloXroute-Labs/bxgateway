class EthProtocolMessageType(object):
    AUTH = "auth"
    AUTH_ACK = "auth_ack"
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


