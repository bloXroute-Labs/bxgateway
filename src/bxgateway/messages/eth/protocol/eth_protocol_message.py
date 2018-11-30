from bxgateway.messages.eth.abstract_eth_message import AbstractEthMessage


class EthProtocolMessage(AbstractEthMessage):
    def __init__(self, msg_bytes, *args, **kwargs):
        super(EthProtocolMessage, self).__init__(msg_bytes, *args, **kwargs)
