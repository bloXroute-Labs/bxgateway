from typing import List

import blxr_rlp as rlp
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody
from bxcommon.utils.blockchain_utils.eth import rlp_utils, eth_common_utils
from bxutils.logging.log_level import LogLevel


class BlockBodiesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.BLOCK_BODIES

    fields = [("blocks", rlp.sedes.CountableList(TransientBlockBody))]

    def __init__(self, msg_bytes, *args, **kwargs):
        super(BlockBodiesEthProtocolMessage, self).__init__(msg_bytes, *args, **kwargs)

        self._block_body_bytes = None

    def __repr__(self):
        return f"BlockBodiesEthProtocolMessage<bodies_count: {len(self.get_block_bodies_bytes())}>"

    def get_blocks(self) -> List[TransientBlockBody]:
        return self.get_field_value("blocks")

    def get_block_bodies_bytes(self):
        if self._block_body_bytes:
            return self._block_body_bytes

        if self._memory_view is None:
            self.serialize()

        self._block_body_bytes = rlp_utils.get_first_list_field_items_bytes(self._memory_view)

        return self._block_body_bytes

    def get_block_transaction_hashes(self, block_index: int) -> List[Sha256Hash]:
        tx_hashes = []

        for tx_bytes in self.get_block_transaction_bytes(block_index):
            tx_hash_bytes = eth_common_utils.keccak_hash(tx_bytes)
            tx_hashes.append(Sha256Hash(tx_hash_bytes))

        return tx_hashes

    def get_block_transaction_bytes(self, block_index: int) -> List[memoryview]:
        block_body_bytes = self.get_block_bodies_bytes()

        if block_index >= len(block_body_bytes):
            return []

        txs_bytes = rlp_utils.get_first_list_field_items_bytes(block_body_bytes[block_index])[0]

        return rlp_utils.get_first_list_field_items_bytes(txs_bytes)

    @classmethod
    def from_body_bytes(cls, body_bytes: memoryview) -> "BlockBodiesEthProtocolMessage":
        bodies_list_prefix = rlp_utils.get_length_prefix_list(len(body_bytes))

        msg_bytes = bytearray(len(bodies_list_prefix) + len(body_bytes))
        msg_bytes[:len(bodies_list_prefix)] = bodies_list_prefix
        msg_bytes[len(bodies_list_prefix):] = body_bytes

        return cls(msg_bytes)

    def log_level(self):
        return LogLevel.DEBUG
