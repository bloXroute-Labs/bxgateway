from typing import Optional

import blxr_rlp as rlp
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxutils.logging.log_level import LogLevel

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class GetBlockHeadersEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_BLOCK_HEADERS

    fields = [("block_hash", rlp.sedes.binary),
              ("amount", rlp.sedes.big_endian_int),
              ("skip", rlp.sedes.big_endian_int),
              ("reverse", rlp.sedes.big_endian_int)]

    def __repr__(self):
        block_hash = self.get_block_hash()
        if block_hash is None:
            block_requested = f"block_number: {self.get_block_number()}"
        else:
            block_requested = f"block_hash: {block_hash}"
        return f"GetBlockHeadersEthProtocolMessage<{block_requested}, " \
            f"amount: {self.get_amount()}, skip: {self.get_skip()}, reverse: {self.get_reverse()}>"

    def log_level(self):
        return LogLevel.DEBUG

    def get_block_hash(self) -> Optional[Sha256Hash]:
        block_hash_bytes = self.get_field_value("block_hash")
        if len(block_hash_bytes) != eth_common_constants.BLOCK_HASH_LEN:
            return None
        else:
            return Sha256Hash(block_hash_bytes)

    def get_block_number(self) -> Optional[int]:
        block_hash_bytes = self.get_field_value("block_hash")
        if len(block_hash_bytes) == eth_common_constants.BLOCK_HASH_LEN:
            return None
        else:
            return int.from_bytes(block_hash_bytes, byteorder="big")

    def get_amount(self) -> int:
        return self.get_field_value("amount")

    def get_skip(self) -> int:
        return self.get_field_value("skip")

    def get_reverse(self) -> int:
        return self.get_field_value("reverse")
