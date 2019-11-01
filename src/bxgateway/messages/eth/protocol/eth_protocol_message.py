from bxgateway.messages.eth.abstract_eth_message import AbstractEthMessage


class EthProtocolMessage(AbstractEthMessage):
    def __init__(self, msg_bytes, *args, **kwargs):
        super(EthProtocolMessage, self).__init__(msg_bytes, *args, **kwargs)

    def __repr__(self):
        return "EthProtocolMessage<type: {}>".format(self.__class__.__name__)

    @classmethod
    def unpack(cls, buf):
        pass

    @classmethod
    def validate_payload(cls, buf, unpacked_args):
        pass

