import rlp

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class GetBlockHeadersEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_BLOCK_HEADERS

    fields = [("block_hash", rlp.sedes.binary),
              ("amount", rlp.sedes.big_endian_int),
              ("skip", rlp.sedes.big_endian_int),
              ("reverse", rlp.sedes.big_endian_int)]

    def __repr__(self):
        return f"GetBlockHeadersEthProtocolMessage<block_hash: ${self.get_block_hash()}, amount: ${self.get_amount()}," \
            f"skip: {self.get_skip()}, reverse: {self.get_reverse()}>"

    def get_block_hash(self) -> Sha256Hash:
        return Sha256Hash(self.get_field_value("block_hash"))

    def get_amount(self) -> int:
        return self.get_field_value("amount")

    def get_skip(self) -> int:
        return self.get_field_value("skip")

    def get_reverse(self) -> int:
        return self.get_field_value("reverse")

