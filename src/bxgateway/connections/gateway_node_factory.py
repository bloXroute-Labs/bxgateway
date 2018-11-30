from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode


def get_gateway_node_type(blockchain_protocol, blockchain_network):

    # TODO: This is temporary logic that will be replaced with list of valid protocols and networks from SDN
    if blockchain_protocol == "Ethereum":
        return EthGatewayNode

    return BtcGatewayNode
