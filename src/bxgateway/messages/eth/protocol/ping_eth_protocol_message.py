from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class PingEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.PING

    def should_log_debug(self):
        return True
