import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.block_hash import BlockHash


class NewBlockHashesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.NEW_BLOCK_HASHES

    fields = [("block_hashes", rlp.sedes.CountableList(BlockHash))]

    def get_block_hashes(self):
        return self.get_field_value("block_hashes")
