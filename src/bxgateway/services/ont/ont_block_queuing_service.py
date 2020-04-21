from typing import List, Union, Optional, cast, TYPE_CHECKING

from bxcommon.models.broadcast_message_type import BroadcastMessageType
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import ont_constants
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.consensus_ont_message import ConsensusOntMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.services.abstract_block_queuing_service import (
    AbstractBlockQueuingService,
)
from bxgateway.utils.ont.ont_object_hash import OntObjectHash
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode

logger = logging.get_logger(__name__)


class OntBlockQueuingService(
    # pyre-fixme[24]: Type parameter `Union[BlockOntMessage, ConsensusOntMessage]` violates constraints on
    #  `Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]`
    #  in generic type `AbstractBlockQueuingService`.
    AbstractBlockQueuingService[Union[BlockOntMessage, ConsensusOntMessage], HeadersOntMessage]
):
    """
    Blocks sent to blockchain node only upon request
    """
    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self.node: "OntGatewayNode" = cast("OntGatewayNode", node)

    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: BlockOntMessage
    ) -> HeadersOntMessage:
        return HeadersOntMessage(
            magic=block_message.magic(), headers=[block_message.header()]
        )

    # pyre-fixme[14]: `bxgateway.services.ont.ont_block_queuing_service.OntBlockQueuingService.push`
    #  overrides method defined in `AbstractBlockQueuingService` inconsistently.
    #  Parameter of type `Union[BlockOntMessage, ConsensusOntMessage]` is not a supertype of the
    #  overridden parameter `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]`.
    def push(
        self,
        block_hash: Sha256Hash,
        # pyre-fixme[9]: block_msg is declared to have type `Union[BlockOntMessage, ConsensusOntMessage]`
        #  but is used as type `None`.
        block_msg: Union[BlockOntMessage, ConsensusOntMessage] = None,
        waiting_for_recovery: bool = False,
    ):
        if self.node.opts.is_consensus and isinstance(block_msg, BlockOntMessage):
            return
        # pyre-fixme[6]: Expected `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
        #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]` for 2nd positional only
        #  parameter to call `AbstractBlockQueuingService.push` but got `Union[BlockOntMessage, ConsensusOntMessage]`.
        super().push(block_hash, block_msg, waiting_for_recovery)
        logger.debug("Added block {} to queuing service", block_hash)
        self._clean_block_queue()

        if block_msg is None:
            return

        block_msg.validate_payload(block_msg.buf, block_msg.unpack(block_msg.buf))
        block_hash = cast(OntObjectHash, block_hash)

        if isinstance(block_msg, BlockOntMessage):
            if block_hash in self._blocks and not waiting_for_recovery:
                self.node.update_current_block_height(
                    block_msg.height(), block_hash
                )
                inv_msg = InvOntMessage(
                    magic=block_msg.magic(),
                    inv_type=InventoryOntType.MSG_BLOCK,
                    blocks=[block_hash],
                )
                self.node.send_msg_to_node(inv_msg)
        # pyre-fixme[25]: `block_msg` has type `ConsensusOntMessage`,
        #  assertion `not isinstance(block_msg, bxgateway.messages.ont.consensus_ont_message.ConsensusOntMessage)`
        #  will always fail.
        elif isinstance(block_msg, ConsensusOntMessage):
            if block_hash in self._blocks and not waiting_for_recovery:
                logger.info("Sending consensus message with block hash {} to blockchain node", block_hash)
                # self.node.block_queuing_service.send_block_to_node(block_hash)
                # the above one line is good, except for the wrong broadcast type in BLOCK_SENT_TO_BLOCKCHAIN_NODE
                self.node.send_msg_to_node(block_msg)
                handling_time, relay_desc = self.node.track_block_from_bdn_handling_ended(block_hash)
                if not self.node.opts.track_detailed_sent_messages:
                    block_stats.add_block_event_by_block_hash(
                        block_hash,
                        BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE,
                        network_num=self.node.network_num,
                        broadcast_type=BroadcastMessageType.CONSENSUS,
                        more_info="{} bytes; Handled in {}; R - {}; {}".format(
                            len(block_msg.rawbytes()),
                            stats_format.duration(handling_time),
                            relay_desc,
                            block_msg.extra_stats_data(),
                        ),
                    )


    # pyre-fixme[14]: `bxgateway.services.ont.ont_block_queuing_service.OntBlockQueuingService.send_block_to_node`
    #  overrides method defined in `AbstractBlockQueuingService` inconsistently. Parameter of
    #  type `Optional[Union[BlockOntMessage, ConsensusOntMessage]]` is not a supertype of the overridden parameter
    #  `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]`.
    def send_block_to_node(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[Union[BlockOntMessage, ConsensusOntMessage]] = None,
    ):
        if block_hash not in self._blocks:
            return

        waiting_for_recovery = self._blocks_waiting_for_recovery[block_hash]
        if waiting_for_recovery:
            raise Exception(
                "Block {} is waiting for recovery".format(block_hash)
            )

        block_msg = self._blocks[block_hash]
        # pyre-fixme[25]: `block_msg` has type `None`, assertion `block_msg` will always fail.
        assert block_msg is not None
        super(OntBlockQueuingService, self).send_block_to_node(
            block_hash, block_msg
        )
        # TODO test remove_from_queue and EncBlockReceivedByGatewayFromNetwork stat event
        # self.remove_from_queue(block_hash)

    def update_recovered_block(
        self, block_hash: Sha256Hash, block_msg: Union[BlockOntMessage, ConsensusOntMessage]
    ):
        if block_hash not in self._blocks:
            return

        block_hash = cast(OntObjectHash, block_hash)

        self._blocks_waiting_for_recovery[block_hash] = False
        # pyre-fixme[6]: Expected `Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
        #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]` for 2nd positional only
        #  parameter to call `AbstractBlockQueuingService.store_block_data`
        #  but got `Union[BlockOntMessage, ConsensusOntMessage]`.
        self.store_block_data(block_hash, block_msg)
        if isinstance(block_msg, BlockOntMessage):
            self.node.update_current_block_height(block_msg.height(), block_hash)
            inv_msg = InvOntMessage(
                magic=block_msg.magic(),
                inv_type=InventoryOntType.MSG_BLOCK,
                blocks=[block_hash],
            )
            self.node.send_msg_to_node(inv_msg)
        # pyre-fixme[25]: `block_msg` has type `ConsensusOntMessage`,
        #  assertion `not isinstance(block_msg, bxgateway.messages.ont.consensus_ont_message.ConsensusOntMessage)`
        #  will always fail.
        elif isinstance(block_msg, ConsensusOntMessage):
            self.node.send_msg_to_node(block_msg)

    def mark_blocks_seen_by_blockchain_node(
        self, block_hashes: List[Sha256Hash]
    ):
        for block_hash in block_hashes:
            self.mark_block_seen_by_blockchain_node(block_hash)

    # pyre-fixme[14]: `bxgateway.services.ont.ont_block_queuing_service.OntBlockQueuingService.mark_block_seen_by_
    #  blockchain_node` overrides method defined in `AbstractBlockQueuingService` inconsistently.
    #  Parameter of type `Optional[Union[BlockOntMessage, ConsensusOntMessage]]` is not a supertype of the overridden
    #  parameter `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]`.
    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        block_message: Optional[Union[BlockOntMessage, ConsensusOntMessage]] = None,
    ):
        self._blocks_seen_by_blockchain_node.add(block_hash)

    def _clean_block_queue(self):
        if len(self._block_queue) > ont_constants.ONT_MAX_QUEUED_BLOCKS:
            oldest_block_hash = self._block_queue[0].block_hash
            self.remove(oldest_block_hash)
            self.node.track_block_from_bdn_handling_ended(oldest_block_hash)

    def remove_from_queue(self, block_hash: Sha256Hash) -> int:
        """
        this is similar to the one in abstract_block_queuing_service, with `if block_hash not in self._blocks` removed
        """
        logger.trace("Removing block {} from queue.")

        for index in range(len(self._block_queue)):
            if self._block_queue[index][0] == block_hash:
                del self._block_queue[index]
                del self._blocks_waiting_for_recovery[block_hash]

                return index

        return -1
