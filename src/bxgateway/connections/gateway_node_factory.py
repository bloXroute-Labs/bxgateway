from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxutils import logging

logger = logging.get_logger(__name__)


def get_gateway_node_type(blockchain_protocol):
    # TODO: This is temporary logic that will be replaced with list of valid protocols and networks from SDN
    if blockchain_protocol.lower() == "ethereum":
        return EthGatewayNode

    return BtcGatewayNode
