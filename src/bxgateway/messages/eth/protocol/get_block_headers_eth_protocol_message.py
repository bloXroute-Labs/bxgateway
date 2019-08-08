import rlp

from bxcommon.utils import convert
from bxcommon.utils.log_level import LogLevel
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class GetBlockHeadersEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_BLOCK_HEADERS

    fields = [("block_hash", rlp.sedes.binary),
              ("amount", rlp.sedes.big_endian_int),
              ("skip", rlp.sedes.big_endian_int),
              ("reverse", rlp.sedes.big_endian_int)]

    def __repr__(self):
        return f"GetBlockHeadersEthProtocolMessage<block_hash: ${convert.bytes_to_hex(self.get_block_hash())}," \
            f"amount: ${self.get_amount()}, skip: {self.get_skip()}, reverse: {self.get_reverse()}>"

    def log_level(self):
        return LogLevel.INFO

    def get_block_hash(self) -> memoryview:
        # Note that the field may be empty, or block number provided instead of hash by ETH node
        return self.get_field_value("block_hash")

    def get_amount(self) -> int:
        return self.get_field_value("amount")

    def get_skip(self) -> int:
        return self.get_field_value("skip")

    def get_reverse(self) -> int:
        return self.get_field_value("reverse")
