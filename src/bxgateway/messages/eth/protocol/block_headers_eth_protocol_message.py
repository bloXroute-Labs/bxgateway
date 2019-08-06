from typing import List

import rlp

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.block_header import BlockHeader


class BlockHeadersEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.BLOCK_HEADERS

    fields = [("block_headers", rlp.sedes.CountableList(BlockHeader))]

    def __repr__(self):
        return f"BlockHeadersEthProtocolMessage<headers: {[header for header in self.get_block_headers()]}>"

    def get_block_headers(self) -> List[BlockHeader]:
        return self.get_field_value("block_headers")


