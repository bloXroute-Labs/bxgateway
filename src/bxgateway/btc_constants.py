BTC_MAGIC_NUMBERS = {
    "main": 0xD9B4BEF9,
    "testnet": 0xDAB5BFFA,
    "testnet3": 0x0709110B,
    "regtest": 0xDAB5BFFA,
    "namecoin": 0xFEB4BEF9
}

# The length of everything in the header minus the checksum
BTC_HEADER_MINUS_CHECKSUM = 20
BTC_HDR_COMMON_OFF = 24  # type: int
BTC_BLOCK_HDR_SIZE = 81
# Length of a sha256 hash
BTC_SHA_HASH_LEN = 32
BTC_IP_ADDR_PORT_SIZE = 18

# The services that we provide
# 1: can ask for full blocks.
# 0x20: Node that is compatible with the hard fork.
BTC_CASH_SERVICE_BIT = 0x20  # Bitcoin cash service bit
BTC_NODE_SERVICES = 1
BTC_CASH_SERVICES = 33

BTC_OBJTYPE_TX = 1
BTC_OBJTYPE_BLOCK = 2
BTC_OBJTYPE_FILTERED_BLOCK = 3

BTC_HELLO_MESSAGES = ["version", "verack"]
