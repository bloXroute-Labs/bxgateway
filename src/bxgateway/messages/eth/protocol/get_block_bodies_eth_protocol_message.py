import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class GetBlockBodiesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_BLOCK_BODIES

    fields = [("block_hashes", rlp.sedes.CountableList(rlp.sedes.binary))]

    def get_block_hashes(self):
        return self.get_field_value("block_hashes")
