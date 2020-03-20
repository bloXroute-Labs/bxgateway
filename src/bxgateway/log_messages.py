from enum import Enum


class GatewayWarningMessage(Enum):
    NO_GATEWAY_PEERS = "no_gateway_peers"


class GatewayErrorMessage(Enum):
    NO_ACTIVE_CONNECTIONS = "no_active_connections"
    UNEXPECTED_TXS_ON_NON_RELAY_CONN = "unexpected_txs_on_non_relay_connection"
    UNEXPECTED_BLOCK_ON_NON_RELAY_CONN = "unexpected_block_on_non_relay_connection"