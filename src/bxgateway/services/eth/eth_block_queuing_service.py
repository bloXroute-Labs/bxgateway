from typing import TYPE_CHECKING, Dict

from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.services.block_queuing_service import BlockQueuingService

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class EthBlockQueuingService(BlockQueuingService[NewBlockEthProtocolMessage]):
    block_checking_alarms: Dict[Sha256Hash, AlarmId]

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self.block_checking_alarms = {}

    def get_previous_block_hash_from_message(self, block_message: NewBlockEthProtocolMessage) -> Sha256Hash:
        return block_message.get_previous_block()

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
