import time
from typing import List

from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService
from bxutils import logging

from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxgateway import ont_constants
from bxgateway.utils.ont.ont_object_hash import OntObjectHash
from bxcommon.utils.stats.block_statistics_service import block_stats

from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage

logger = logging.get_logger(__name__)


class OntBlockQueuingService(AbstractBlockQueuingService[BlockOntMessage]):
    """
    Blocks sent to blockchain node only upon request
    """

    def push(self, block_hash: OntObjectHash, block_msg: BlockOntMessage = None, waiting_for_recovery: bool = False):
        if not self.can_add_block_to_queuing_service(block_hash, block_msg, waiting_for_recovery):
            return

        logger.debug("Adding block {} to queuing service", block_hash)
        self._block_queue.append((block_hash, time.time()))
        self._blocks[block_hash] = (waiting_for_recovery, block_msg)
        self._clean_block_queue()

        if block_hash in self._blocks and not waiting_for_recovery:
            self.node.update_current_block_height(block_msg.height(), block_hash)
            inv_msg = InvOntMessage(magic=block_msg.magic(), inv_type=InventoryOntType.MSG_BLOCK,
                                    blocks=[block_hash])
            self.node.send_msg_to_node(inv_msg)

    def remove(self, block_hash: OntObjectHash):
        if block_hash not in self._blocks:
            return

        for index in range(len(self._block_queue)):
            if self._block_queue[index][0] == block_hash:
                del self._block_queue[index]
                del self._blocks[block_hash]

    def send_block_to_node(self, block_hash: OntObjectHash):
        if block_hash not in self._blocks:
            return

        waiting_for_recovery, block_msg = self._blocks[block_hash]
        if waiting_for_recovery:
            raise Exception("Block {} is waiting for recovery".format(block_hash))
        super(OntBlockQueuingService, self)._send_block_to_node(block_hash, block_msg)

    def send_header_to_node(self, block_hash: OntObjectHash) -> bool:
        if block_hash not in self._blocks:
            return False

        block_msg = self._blocks[block_hash][1]
        header_msg = HeadersOntMessage(magic=block_msg.magic(), headers=[block_msg.header()])
        self.node.send_msg_to_node(header_msg)
        block_stats.add_block_event_by_block_hash(block_hash, BlockStatEventType.BLOCK_HEADER_SENT_TO_BLOCKCHAIN_NODE,
                                                  network_num=self.node.network_num)
        return True

    def update_recovered_block(self, block_hash: OntObjectHash, block_msg: BlockOntMessage):
        if block_hash not in self._blocks:
            return

        self._blocks[block_hash] = (False, block_msg)
        self.node.update_current_block_height(block_msg.height(), block_hash)
        inv_msg = InvOntMessage(magic=block_msg.magic(), inv_type=InventoryOntType.MSG_BLOCK,
                                blocks=[block_hash])
        self.node.send_msg_to_node(inv_msg)

    def mark_blocks_seen_by_blockchain_node(self, block_hashes: List[OntObjectHash]):
        for block_hash in block_hashes:
            self.mark_block_seen_by_blockchain_node(block_hash)

    def mark_block_seen_by_blockchain_node(self, block_hash: OntObjectHash):
        self._blocks_seen_by_blockchain_node.add(block_hash)

    def _clean_block_queue(self):
        if len(self._block_queue) > ont_constants.ONT_MAX_QUEUED_BLOCKS:
            old_block_hash = self._block_queue.popleft()[0]
            del self._blocks[old_block_hash]
            self.node.track_block_from_bdn_handling_ended(old_block_hash)
