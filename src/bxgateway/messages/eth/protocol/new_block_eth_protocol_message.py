import rlp

from bxcommon.utils.object_hash import ObjectHash
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.block import Block
from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.utils.eth import rlp_utils, crypto_utils


class NewBlockEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.NEW_BLOCK

    fields = [("block", Block),
              ("chain_difficulty", rlp.sedes.big_endian_int)]

    block = None
    chain_difficulty = None

    def __init__(self, msg_bytes, *args, **kwargs):
        super(NewBlockEthProtocolMessage, self).__init__(msg_bytes, *args, **kwargs)

        self._block_hash = None
        self._timestamp = None

    def get_block(self):
        return self.get_field_value("block")

    def get_chain_difficulty(self):
        return self.get_field_value("chain_difficulty")

    def block_hash(self):
        if self._block_hash is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
            block_hdr_bytes = block_itm_bytes[0:block_hdr_itm_start + block_hdr_itm_len]

            raw_hash = crypto_utils.keccak_hash(block_hdr_bytes)

            self._block_hash = ObjectHash(raw_hash)

        return self._block_hash

    def timestamp(self):
        """
        :return: seconds since epoch
        """
        if self._timestamp is None:
            _, block_msg_itm_len, block_msg_itm_start = rlp_utils.consume_length_prefix(self._memory_view, 0)
            block_msg_bytes = self._memory_view[block_msg_itm_start:block_msg_itm_start + block_msg_itm_len]

            _, block_itm_len, block_itm_start = rlp_utils.consume_length_prefix(block_msg_bytes, 0)
            block_itm_bytes = block_msg_bytes[block_msg_itm_start:block_msg_itm_start + block_itm_len]

            _, block_hdr_itm_len, block_hdr_itm_start = rlp_utils.consume_length_prefix(block_itm_bytes, 0)
            block_hdr_bytes = block_itm_bytes[block_hdr_itm_start:block_hdr_itm_start + block_hdr_itm_len]

            offset = BlockHeader.FIXED_LENGTH_FIELD_OFFSET
            _difficulty, difficulty_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += difficulty_length
            _number, number_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += number_length
            _gas_limit, gas_limit_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += gas_limit_length
            _gas_used, gas_used_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            offset += gas_used_length

            timestamp, _timestamp_length = rlp_utils.decode_int(block_hdr_bytes, offset)
            self._timestamp = timestamp

        return self._timestamp
