import rlp

from bxutils.logging.log_level import LogLevel

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.utils.eth import rlp_utils


class GetBlockBodiesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_BLOCK_BODIES

    fields = [("block_hashes", rlp.sedes.CountableList(rlp.sedes.binary))]

    def __repr__(self):
        return f"GetBlockBodiesEthProtocolMessage<bodies_count: {len(self.get_block_hashes())}>"

    def get_block_hashes(self):
        if self._memory_view is None:
            self.serialize()

        return rlp_utils.get_first_list_field_items_bytes(self._memory_view, remove_items_length_prefix=True)

    def log_level(self):
        return LogLevel.INFO
