from typing import List

import blxr_rlp as rlp
from bxutils.logging.log_level import LogLevel

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxcommon.messages.eth.serializers.block_header import BlockHeader
from bxcommon.utils.blockchain_utils.eth import rlp_utils


class BlockHeadersEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.BLOCK_HEADERS

    fields = [("block_headers", rlp.sedes.CountableList(BlockHeader))]

    def __repr__(self):
        headers = self.get_block_headers()
        headers_repr = list(headers[:1])
        if len(headers) > 1:
            headers_repr.append(headers[-1])
        return f"BlockHeadersEthProtocolMessage<headers_count: {len(headers)} " \
               f"headers: [{'...'.join([h.hash().hex() for h in headers_repr])}]>"

    def get_block_headers(self) -> List[BlockHeader]:
        return self.get_field_value("block_headers")

    def get_block_headers_bytes(self):
        if self._memory_view is None:
            self.serialize()

        return rlp_utils.get_first_list_field_items_bytes(self._memory_view)

    @classmethod
    def from_header_bytes(cls, header_bytes: memoryview) -> "BlockHeadersEthProtocolMessage":
        headers_list_prefix = rlp_utils.get_length_prefix_list(len(header_bytes))

        msg_bytes = bytearray(len(headers_list_prefix) + len(header_bytes))
        msg_bytes[:len(headers_list_prefix)] = headers_list_prefix
        msg_bytes[len(headers_list_prefix):] = header_bytes

        return cls(msg_bytes)

    def log_level(self):
        return LogLevel.DEBUG
