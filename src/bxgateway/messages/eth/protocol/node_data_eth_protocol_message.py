import blxr_rlp as rlp
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class NodeDataEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.NODE_DATA

    fields = [("raw_data", rlp.sedes.raw)]

    def get_raw_data(self):
        return self.get_field_value("raw_data")