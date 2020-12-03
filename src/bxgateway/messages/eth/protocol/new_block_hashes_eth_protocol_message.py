import blxr_rlp as rlp
from typing import List, Tuple

from bxutils.logging.log_level import LogLevel

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.block_hash import BlockHash
from bxcommon.utils.blockchain_utils.eth import rlp_utils


class NewBlockHashesEthProtocolMessage(EthProtocolMessage, AbstractBlockMessage):
    msg_type = EthProtocolMessageType.NEW_BLOCK_HASHES

    fields = [("block_hash_number_pairs", rlp.sedes.CountableList(BlockHash))]

    def __repr__(self) -> str:
        return f"NewBlockHashesEthProtocolMessage<block_hashes: [{self.get_block_hash_number_pairs()}]"

    def block_hash(self) -> Sha256Hash:
        """
        Returns the first block hash in the message

        This is used when send new block hash to the blockchain node. Always send a single block in that message.
        :return:
        """

        block_hash_number_pairs = self.get_field_value("block_hash_number_pairs")
        return Sha256Hash(block_hash_number_pairs[0].hash)

    def timestamp(self) -> int:
        raise NotImplementedError()

    def extra_stats_data(self) -> str:
        return "Block hash"

    def get_block_hash_number_pairs(self) -> List[Tuple[Sha256Hash, int]]:
        return [(Sha256Hash(block_hash.hash), block_hash.number) for block_hash in
                self.get_field_value("block_hash_number_pairs")]

    @classmethod
    def from_block_hash_number_pair(cls, block_hash: Sha256Hash, number: int) -> "NewBlockHashesEthProtocolMessage":
        block_hash_bytes = block_hash.binary
        msg_size = len(block_hash_bytes)

        block_hash_prefix = rlp_utils.get_length_prefix_str(len(block_hash_bytes))
        msg_size += len(block_hash_prefix)

        number_bytes = rlp_utils.encode_int(number)
        msg_size += len(number_bytes)

        block_hash_and_number_prefix = rlp_utils.get_length_prefix_list(msg_size)
        msg_size += len(block_hash_and_number_prefix)

        block_hashes_list_prefix = rlp_utils.get_length_prefix_list(msg_size)
        msg_size += len(block_hashes_list_prefix)

        msg_bytes = bytearray(msg_size)

        offset = 0

        msg_bytes[offset:len(block_hashes_list_prefix)] = block_hashes_list_prefix
        offset += len(block_hashes_list_prefix)

        msg_bytes[offset:offset + len(block_hash_and_number_prefix)] = block_hash_and_number_prefix
        offset += len(block_hash_and_number_prefix)

        msg_bytes[offset:offset + len(block_hash_prefix)] = block_hash_prefix
        offset += len(block_hash_prefix)

        msg_bytes[offset:offset + len(block_hash_bytes)] = block_hash_bytes
        offset += len(block_hash_bytes)

        msg_bytes[offset:offset + len(number_bytes)] = number_bytes

        return cls(msg_bytes)

    def log_level(self) -> LogLevel:
        return LogLevel.DEBUG

    def prev_block_hash(self) -> Sha256Hash:
        raise NotImplementedError()

    def txns(self):
        raise NotImplementedError()