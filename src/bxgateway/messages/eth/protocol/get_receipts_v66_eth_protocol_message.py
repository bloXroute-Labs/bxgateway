import blxr_rlp as rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_eth_protocol_message import \
    GetReceiptsEthProtocolMessage


class GetReceiptsV66EthProtocolMessage(GetReceiptsEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(GetReceiptsEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"<GetReceiptsEthProtocolMessage {len(self.get_block_hashes())} request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> GetReceiptsEthProtocolMessage:
        return GetReceiptsEthProtocolMessage(None, self.get_block_hashes())
