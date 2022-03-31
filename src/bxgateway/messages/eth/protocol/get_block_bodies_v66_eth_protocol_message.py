import blxr_rlp as rlp
from bxcommon.utils import convert

from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import \
    GetBlockBodiesEthProtocolMessage


class GetBlockBodiesV66EthProtocolMessage(GetBlockBodiesEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
    ]
    fields.extend(GetBlockBodiesEthProtocolMessage.fields)

    def __repr__(self):
        requested_hashes = self.get_field_value("block_hashes")
        request_repr = list(requested_hashes[:1])
        if len(requested_hashes) > 1:
            request_repr.append(requested_hashes[-1])
        request_repr = map(convert.bytes_to_hex, request_repr)
        return (
            f"GetBlockBodiesEthProtocolMessage<"
            f"bodies_count: {len(requested_hashes)}, "
            f"hashes: [{'...'.join([requested_hash for requested_hash in request_repr])}], "
            f"request_id: {self.get_request_id()}>"
        )

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> GetBlockBodiesEthProtocolMessage:
        return GetBlockBodiesEthProtocolMessage(None, self.get_block_hashes())
