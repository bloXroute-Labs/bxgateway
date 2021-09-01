import blxr_rlp as rlp

from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import \
    BlockHeadersEthProtocolMessage


class BlockHeadersV66EthProtocolMessage(BlockHeadersEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(BlockHeadersEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"{repr(self.get_block_headers())}<request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> BlockHeadersEthProtocolMessage:
        return BlockHeadersEthProtocolMessage(None, self.get_block_headers())

