from typing import TYPE_CHECKING, Dict, Optional
from collections import defaultdict

from bxcommon import constants
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import eth_constants, gateway_constants
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import \
    BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import (
    BlockHeadersEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import (
    GetBlockHeadersEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import (
    NewBlockEthProtocolMessage,
)
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import (
    NewBlockHashesEthProtocolMessage,
)
from bxgateway.services.push_block_queuing_service import (
    PushBlockQueuingService,
)

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class EthBlockQueuingService(
    PushBlockQueuingService[
        InternalEthBlockInfo, BlockHeadersEthProtocolMessage
    ]
):
    block_checking_alarms: Dict[Sha256Hash, AlarmId]
    block_repeat_count: Dict[Sha256Hash, int]

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self.block_checking_alarms = {}
        self._sent_new_block_headers: ExpiringDict[
            Sha256Hash, NewBlockParts
        ] = ExpiringDict(
            node.alarm_queue, gateway_constants.MAX_BLOCK_CACHE_TIME_S
        )
        self.block_repeat_count = defaultdict(int)

    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: InternalEthBlockInfo
    ) -> BlockHeadersEthProtocolMessage:
        if block_hash in self._sent_new_block_headers:
            block_header_bytes = self._sent_new_block_headers[block_hash].block_header_bytes
        else:
            block_header_bytes = (
                block_message.to_new_block_parts().block_header_bytes
            )
        return BlockHeadersEthProtocolMessage.from_header_bytes(
            block_header_bytes
        )

    def send_block_to_node(
        self, block_hash: Sha256Hash, block_msg: InternalEthBlockInfo
    ):

        new_block_parts = block_msg.to_new_block_parts()
        self._sent_new_block_headers.add(block_hash, new_block_parts)

        if block_msg.has_total_difficulty():
            new_block_msg = block_msg.to_new_block_msg()
            super(EthBlockQueuingService, self).send_block_to_node(
                block_hash, new_block_msg
            )
            self.node.set_known_total_difficulty(
                new_block_msg.block_hash(), new_block_msg.chain_difficulty()
            )
        else:
            calculated_total_difficulty = self.node.try_calculate_total_difficulty(
                block_hash, new_block_parts
            )

            if calculated_total_difficulty is None:
                # Total difficulty may be unknown after a long fork or
                # if gateways just started. Announcing new block hashes to
                # ETH node in that case. It
                # will request header and body separately.
                new_block_headers_msg = NewBlockHashesEthProtocolMessage.from_block_hash_number_pair(
                    block_hash, new_block_parts.block_number
                )
                super(EthBlockQueuingService, self).send_block_to_node(
                    block_hash, new_block_headers_msg
                )
            else:
                new_block_msg = NewBlockEthProtocolMessage.from_new_block_parts(
                    new_block_parts, calculated_total_difficulty
                )
                super(EthBlockQueuingService, self).send_block_to_node(
                    block_hash, new_block_msg
                )

    def get_previous_block_hash_from_message(
        self, block_message: NewBlockEthProtocolMessage
    ) -> Sha256Hash:
        return block_message.prev_block_hash()

    def on_block_sent(
        self, block_hash: Sha256Hash, _block_message: NewBlockEthProtocolMessage
    ):
        if block_hash not in self.block_checking_alarms:
            # requires delay to have time to validate and process block
            self.block_checking_alarms[
                block_hash
            ] = self.node.alarm_queue.register_alarm(
                eth_constants.CHECK_BLOCK_RECEIPT_DELAY_S,
                self._check_for_block_on_repeat,
                block_hash,
            )

    def mark_block_seen_by_blockchain_node(self, block_hash: Sha256Hash):
        super().mark_block_seen_by_blockchain_node(block_hash)

        if block_hash in self.block_checking_alarms:
            self.node.alarm_queue.unregister_alarm(
                self.block_checking_alarms[block_hash]
            )
            del self.block_checking_alarms[block_hash]
            self.block_repeat_count.pop(block_hash, 0)

    def remove(self, block_hash: Sha256Hash) -> int:
        index = super().remove(block_hash)
        if block_hash in self._sent_new_block_headers:
            del self._sent_new_block_headers[block_hash]
        return index

    def try_send_body_to_node(self, block_hash: Sha256Hash) -> bool:
        if block_hash not in self._blocks:
            return False

        block_message = self._blocks[block_hash]
        if block_message is None:
            return False

        if block_hash in self._sent_new_block_headers:
            block_body_bytes = self._sent_new_block_headers[block_hash].block_body_bytes
        else:
            block_body_bytes = (
                block_message.to_new_block_parts().block_body_bytes
            )

        block_body_msg = BlockBodiesEthProtocolMessage.from_body_bytes(
            block_body_bytes
        )
        self.node.send_msg_to_node(block_body_msg)
        block_stats.add_block_event_by_block_hash(
            block_hash,
            BlockStatEventType.BLOCK_HEADER_SENT_TO_BLOCKCHAIN_NODE,
            network_num=self.node.network_num,
        )
        return True

    def _check_for_block_on_repeat(self, block_hash: Sha256Hash) -> float:
        get_confirmation_message = GetBlockHeadersEthProtocolMessage(
            None, block_hash.binary, 1, 0, 0
        )

        self.node.send_msg_to_node(get_confirmation_message)

        if self.block_repeat_count[block_hash] < 5:
            self.block_repeat_count[block_hash] += 1
            return eth_constants.CHECK_BLOCK_RECEIPT_INTERVAL_S
        else:
            del self.block_repeat_count[block_hash]
            return constants.CANCEL_ALARMS
