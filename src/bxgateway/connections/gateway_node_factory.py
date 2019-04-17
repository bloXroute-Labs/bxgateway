from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode


def get_gateway_node_type(blockchain_protocol, blockchain_network):

    # TODO: This is temporary logic that will be replaced with list of valid protocols and networks from SDN
    if blockchain_protocol == "Ethereum":
        from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
        return EthGatewayNode

    return BtcGatewayNode
