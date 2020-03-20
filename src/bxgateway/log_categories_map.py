from bxgateway.log_messages import GatewayErrorMessage, GatewayWarningMessage
from bxutils.log_message_categories import LogMessageCategories
from bxutils.log_categories_map import categories_map
from bxgateway.log_message_categories import GatewayLogMessageCategories

gateway_categories_map = {
    GatewayErrorMessage.NO_ACTIVE_CONNECTIONS:  LogMessageCategories.NO_PEERS,
    GatewayErrorMessage.UNEXPECTED_BLOCK_ON_NON_RELAY_CONN: GatewayLogMessageCategories.UNEXPECTED_MESSAGE,
    GatewayErrorMessage.UNEXPECTED_TXS_ON_NON_RELAY_CONN: GatewayLogMessageCategories.UNEXPECTED_MESSAGE,
    GatewayWarningMessage.NO_GATEWAY_PEERS: LogMessageCategories.NO_PEERS
}

categories_map.update(gateway_categories_map)
