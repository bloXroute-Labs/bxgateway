from typing import TYPE_CHECKING, Dict, Optional

from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import NewBlockHashesEthProtocolMessage
from bxgateway.services.block_queuing_service import BlockQueuingService

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class EthBlockQueuingService(BlockQueuingService[NewBlockEthProtocolMessage]):
    block_checking_alarms: Dict[Sha256Hash, AlarmId]

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self.block_checking_alarms = {}
        self._sent_new_block_headers: ExpiringDict[Sha256Hash, NewBlockParts] = \
            ExpiringDict(node.alarm_queue, eth_constants.SENT_NEW_BLOCK_HEADERS_EXPIRE_TIME_S)

    def _send_block_to_node(self, block_hash: Sha256Hash, block_msg: InternalEthBlockInfo):

        if block_msg.has_total_difficulty():
            new_block_msg = block_msg.to_new_block_msg()
            super(EthBlockQueuingService, self)._send_block_to_node(block_hash, new_block_msg)
            self.node.set_known_total_difficulty(new_block_msg.block_hash(), new_block_msg.chain_difficulty())
        else:
            new_block_parts = block_msg.to_new_block_parts()
            calculated_total_difficulty = self.node.try_calculate_total_difficulty(block_hash, new_block_parts)

            if calculated_total_difficulty is None:
                # Total difficulty may be unknown after a long fork or if gateways just started.
                # Announcing new block hashes to ETH node in that case. It will request header and body separately.
                new_block_headers_msg = NewBlockHashesEthProtocolMessage.from_block_hash_number_pair(block_hash,
                                                                                                     new_block_parts.block_number)
                self._sent_new_block_headers.add(block_hash, new_block_parts)

                super(EthBlockQueuingService, self)._send_block_to_node(block_hash, new_block_headers_msg)
            else:
                new_block_msg = NewBlockEthProtocolMessage.from_new_block_parts(new_block_parts,
                                                                                calculated_total_difficulty)
                super(EthBlockQueuingService, self)._send_block_to_node(block_hash, new_block_msg)

    def get_sent_new_block_header(self, block_hash: Sha256Hash) -> Optional[memoryview]:

        if block_hash in self._sent_new_block_headers.contents:
            return self._sent_new_block_headers.contents[block_hash].block_header_bytes

        return None

    def get_sent_new_block_body(self, block_hash: Sha256Hash) -> Optional[memoryview]:

        if block_hash in self._sent_new_block_headers.contents:
            return self._sent_new_block_headers.contents[block_hash].block_body_bytes

        return None

    def get_previous_block_hash_from_message(self, block_message: NewBlockEthProtocolMessage) -> Sha256Hash:
        return block_message.prev_block_hash()

    def on_block_sent(self, block_hash: Sha256Hash, _block_message: NewBlockEthProtocolMessage):
        if block_hash not in self.block_checking_alarms:
            # requires delay to have time to validate and process block
            self.block_checking_alarms[block_hash] = \
                self.node.alarm_queue.register_alarm(
                    eth_constants.CHECK_BLOCK_RECEIPT_DELAY_S, self._check_for_block_on_repeat, block_hash)

    def mark_block_seen_by_blockchain_node(self, block_hash: Sha256Hash):
        super().mark_block_seen_by_blockchain_node(block_hash)
        if block_hash in self.block_checking_alarms:
            self.node.alarm_queue.unregister_alarm(self.block_checking_alarms[block_hash])

    def _check_for_block_on_repeat(self, block_hash: Sha256Hash) -> float:
        get_confirmation_message = GetBlockHeadersEthProtocolMessage(None, block_hash.binary, 1, 0, 0)
        self.node.send_msg_to_node(get_confirmation_message)
        return eth_constants.CHECK_BLOCK_RECEIPT_INTERVAL_S
