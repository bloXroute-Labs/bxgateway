import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class GetNodeDataEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_NODE_DATA

    fields = [("hashes", rlp.sedes.CountableList(rlp.sedes.binary))]

    def get_hashes(self):
        return self.get_field_value("hashes")