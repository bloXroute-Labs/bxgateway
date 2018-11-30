import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody


class BlockBodiesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.BLOCK_BODIES

    fields = [("blocks", rlp.sedes.CountableList(TransientBlockBody))]
    
    def get_blocks(self):
        return self.get_field_value("blocks")
