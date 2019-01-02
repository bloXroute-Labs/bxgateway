import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class GetBlockHeadersEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_BLOCK_HEADERS

    fields = [("block", rlp.sedes.binary),
              ("amount", rlp.sedes.big_endian_int),
              ("skip", rlp.sedes.big_endian_int),
              ("reverse", rlp.sedes.big_endian_int)]

    def get_block(self):
        return self.get_field_value("block")

    def get_amount(self):
        return self.get_field_value("amount")

    def get_skip(self):
        return self.get_field_value("skip")

    def get_reverse(self):
        return self.get_field_value("reverse")
