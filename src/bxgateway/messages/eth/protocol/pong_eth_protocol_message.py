from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class PongEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.PONG

    def should_log_debug(self):
        return True
