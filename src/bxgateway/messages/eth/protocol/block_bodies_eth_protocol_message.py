import rlp

from bxutils.logging.log_level import LogLevel

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody
from bxgateway.utils.eth import rlp_utils


class BlockBodiesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.BLOCK_BODIES

    fields = [("blocks", rlp.sedes.CountableList(TransientBlockBody))]
    
    def __repr__(self):
        return f"BlockBodiesEthProtocolMessage<bodies_count: {len(self.get_block_bodies_bytes())}>"

    def get_blocks(self):
        return self.get_field_value("blocks")

    def get_block_bodies_bytes(self):
        if self._memory_view is None:
            self.serialize()

        return rlp_utils.get_first_list_field_items_bytes(self._memory_view)

    @classmethod
    def from_body_bytes(cls, body_bytes: memoryview) -> "BlockBodiesEthProtocolMessage":
        bodies_list_prefix = rlp_utils.get_length_prefix_list(len(body_bytes))

        msg_bytes = bytearray(len(bodies_list_prefix) + len(body_bytes))
        msg_bytes[:len(bodies_list_prefix)] = bodies_list_prefix
        msg_bytes[len(bodies_list_prefix):] = body_bytes

        return cls(msg_bytes)

    def log_level(self):
        return LogLevel.INFO

