from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage


class RawEthProtocolMessage(EthProtocolMessage):
    def __init__(self, msg_bytes):
        if not msg_bytes:
            raise ValueError("Message bytes required for raw message")

        super(EthProtocolMessage, self).__init__(msg_bytes)

    def serialize(self):
        raise NotImplementedError("Raw message can't be serialized")

    def deserialize(self):
        raise NotImplementedError("Raw message can't be deserialized")

