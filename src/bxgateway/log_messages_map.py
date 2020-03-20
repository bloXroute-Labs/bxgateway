from bxgateway.log_messages import GatewayErrorMessage, GatewayWarningMessage



gateway_message_map = {
    GatewayWarningMessage.NO_GATEWAY_PEERS: "Did not receive expected gateway peers from BDN. Retrying.",
    GatewayErrorMessage.NO_ACTIVE_CONNECTIONS: ("Gateway does not have an active connection to the relay network. "
                                                "There may be issues with the BDN. Exiting."),
    GatewayErrorMessage.UNEXPECTED_TXS_ON_NON_RELAY_CONN: "Received unexpected txs message on non-tx relay connection: {}",
    GatewayErrorMessage.UNEXPECTED_BLOCK_ON_NON_RELAY_CONN: "Received unexpected block message on non-block relay connection: {}"
}



