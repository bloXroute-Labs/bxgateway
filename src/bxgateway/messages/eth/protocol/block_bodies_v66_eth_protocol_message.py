import blxr_rlp as rlp

from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage


class BlockBodiesV66EthProtocolMessage(BlockBodiesEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(BlockBodiesEthProtocolMessage.fields)

    def __repr__(self):
        return (
            f"{repr(self.get_message())[:-1]}, request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> BlockBodiesEthProtocolMessage:
        return BlockBodiesEthProtocolMessage(None, self.get_blocks())
