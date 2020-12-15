import blxr_rlp as rlp
from blxr_rlp.sedes import CountableList

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class DisconnectEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.DISCONNECT

    fields = [("reason", CountableList(rlp.sedes.big_endian_int))]

    def get_reason(self):
        return self.get_field_value("reason")[0]
