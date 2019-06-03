from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter


class EthRelayConnection(AbstractRelayConnection):
    def __init__(self, sock, address, node, from_me=False):
        super(EthRelayConnection, self).__init__(sock, address, node, from_me=from_me)

        self.message_converter = EthMessageConverter()
