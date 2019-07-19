import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class ReceiptsEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.RECEIPTS

    fields = [("raw_data", rlp.sedes.raw)]

    def get_raw_data(self):
        return self.get_field_value("raw_data")