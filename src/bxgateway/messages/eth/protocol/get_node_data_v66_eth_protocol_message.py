import blxr_rlp as rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.get_node_data_eth_protocol_message import \
    GetNodeDataEthProtocolMessage


class GetNodeDataV66EthProtocolMessage(GetNodeDataEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(GetNodeDataEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"{repr(self.get_message())}<request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> GetNodeDataEthProtocolMessage:
        return GetNodeDataEthProtocolMessage(None, self.get_hashes())
