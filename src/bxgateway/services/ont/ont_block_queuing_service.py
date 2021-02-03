from typing import List, Union, Optional, cast, TYPE_CHECKING, Iterator

from bxcommon.exceptions import ChecksumError
from bxcommon.models.broadcast_message_type import BroadcastMessageType
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import ont_constants, gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.consensus_ont_message import OntConsensusMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.services.abstract_block_queuing_service import (
    AbstractBlockQueuingService,
)
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode

logger = logging.get_logger(__name__)


class OntBlockQueuingService(
    AbstractBlockQueuingService[Union[BlockOntMessage, OntConsensusMessage], HeadersOntMessage]
):
    """
    Blocks sent to blockchain node only upon request
    """

    _block_hashes_by_height: ExpiringDict[int, Sha256Hash]
    _highest_block_number: int = 0

    def __init__(
        self,
        node: "AbstractGatewayNode",
        connection: AbstractGatewayBlockchainConnection
    ):
        super().__init__(node, connection)
        self.node: "OntGatewayNode" = cast("OntGatewayNode", node)
        self._block_hashes_by_height = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "ont_block_queue_hashes_by_heights",
        )

    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: BlockOntMessage
    ) -> HeadersOntMessage:
        return HeadersOntMessage(
            magic=block_message.magic(), headers=[block_message.header()]
        )

    # pyre-fixme[14]: `bxgateway.services.ont.ont_block_queuing_service.OntBlockQueuingService.push`
    #  overrides method defined in `AbstractBlockQueuingService` inconsistently.
    #  Parameter of type `Union[BlockOntMessage, OntConsensusMessage]` is not a supertype of the
    #  overridden parameter `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]`.
    def push(
        self,
        block_hash: Sha256Hash,
        # pyre-fixme[9]: block_msg is declared to have type `Union[BlockOntMessage, OntConsensusMessage]`
        #  but is used as type `None`.
        block_msg: Union[BlockOntMessage, OntConsensusMessage] = None,
        waiting_for_recovery: bool = False,
    ):
        if self.node.opts.is_consensus and isinstance(block_msg, BlockOntMessage):
            return
        super().push(block_hash, block_msg, waiting_for_recovery)
        self.connection.log_debug(
            "Added block {} to queuing service (waiting for recovery: {})", block_hash, waiting_for_recovery
        )
        self._clean_block_queue()

        if block_msg is None:
            return

        try:
            block_msg.validate_payload(block_msg.buf, block_msg.unpack(block_msg.buf))
        except ChecksumError:
            logger.debug("Encountered checksum error, which can be caused by duplicate transaction. "
                         "Stop processing block {}", block_hash)
            return
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
                self.connection.enqueue_msg(inv_msg)
        # pyre-fixme[25]: `block_msg` has type `OntConsensusMessage`,
        #  assertion `not isinstance(block_msg, bxgateway.messages.ont.consensus_ont_message.OntConsensusMessage)`
        #  will always fail.
        elif isinstance(block_msg, OntConsensusMessage):
            if block_hash in self._blocks and not waiting_for_recovery:
                self.connection.log_info(
                    "Sending consensus message with block hash {} to blockchain node",
                    block_hash,
                )
                self.connection.enqueue_msg(block_msg)
                handling_time, relay_desc = self.node.track_block_from_bdn_handling_ended(block_hash)
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
    #  type `Optional[Union[BlockOntMessage, OntConsensusMessage]]` is not a supertype of the overridden parameter
    #  `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]`.
    def send_block_to_node(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[Union[BlockOntMessage, OntConsensusMessage]] = None,
    ):
        if block_hash not in self._blocks or block_hash not in self._blocks_waiting_for_recovery:
            return

        waiting_for_recovery = self._blocks_waiting_for_recovery[block_hash]
        if waiting_for_recovery:
            raise Exception(
                "Block {} is waiting for recovery in queuing service for {}".format(block_hash, self.connection.peer_desc)
            )

        block_msg = cast(Union[BlockOntMessage, OntConsensusMessage], self.node.block_storage[block_hash])
        super(OntBlockQueuingService, self).send_block_to_node(
            block_hash, block_msg
        )
        # TODO test remove_from_queue and EncBlockReceivedByGatewayFromNetwork stat event
        self.remove_from_queue(block_hash)

    def update_recovered_block(
        self, block_hash: Sha256Hash, block_msg: Union[BlockOntMessage, OntConsensusMessage]
    ):
        if block_hash not in self._blocks or block_hash not in self._blocks_waiting_for_recovery:
            return

        block_hash = cast(OntObjectHash, block_hash)

        self._blocks_waiting_for_recovery[block_hash] = False
        self.store_block_data(block_hash, block_msg)
        if isinstance(block_msg, BlockOntMessage):
            self.node.update_current_block_height(block_msg.height(), block_hash)
            inv_msg = InvOntMessage(
                magic=block_msg.magic(),
                inv_type=InventoryOntType.MSG_BLOCK,
                blocks=[block_hash],
            )
            self.connection.enqueue_msg(inv_msg)
        # pyre-fixme[25]: `block_msg` has type `OntConsensusMessage`,
        #  assertion `not isinstance(block_msg, bxgateway.messages.ont.consensus_ont_message.OntConsensusMessage)`
        #  will always fail.
        elif isinstance(block_msg, OntConsensusMessage):
            self.connection.enqueue_msg(block_msg)

    def mark_blocks_seen_by_blockchain_node(
        self, block_hashes: List[Sha256Hash]
    ):
        for block_hash in block_hashes:
            self.mark_block_seen_by_blockchain_node(block_hash)

    # pyre-fixme[14]: `bxgateway.services.ont.ont_block_queuing_service.OntBlockQueuingService.mark_block_seen_by_
    #  blockchain_node` overrides method defined in `AbstractBlockQueuingService` inconsistently.
    #  Parameter of type `Optional[Union[BlockOntMessage, OntConsensusMessage]]` is not a supertype of the overridden
    #  parameter `Optional[Variable[bxgateway.services.abstract_block_queuing_service.TBlockMessage
    #  (bound to bxcommon.messages.abstract_block_message.AbstractBlockMessage)]]`.
    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        block_message: Optional[Union[BlockOntMessage, OntConsensusMessage]] = None,
        _block_number: Optional[int] = None,
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
        self.connection.log_trace("Removing block {} from queue.", block_hash)

        for index in range(len(self._block_queue)):
            if self._block_queue[index][0] == block_hash:
                del self._block_queue[index]
                del self._blocks_waiting_for_recovery[block_hash]
                return index

        return -1

    def store_block_data(
            self,
            block_hash: Sha256Hash,
            block_msg: Union[BlockOntMessage, OntConsensusMessage]
    ):
        if isinstance(block_msg, BlockOntMessage):
            block_height = block_msg.height()
            if block_height > self._highest_block_number:
                self._highest_block_number = block_height
            self._block_hashes_by_height[block_height] = block_hash
        super().store_block_data(block_hash, block_msg)

    def iterate_block_hashes_starting_from_hash(
            self,
            block_hash: Sha256Hash,
            max_count: int = gateway_constants.TRACKED_BLOCK_MAX_HASH_LOOKUP) -> Iterator[Sha256Hash]:
        """
        iterate over cached blocks headers in descending order
        :param block_hash: starting block hash
        :param max_count: max number of elements to return
        :return: Iterator of block hashes in descending order
        """
        block_hash_ = block_hash
        for _ in range(max_count):
            if block_hash_ and block_hash_ in self._blocks:
                yield block_hash_
                block_msg = self.node.block_storage[block_hash_]
                assert block_msg is not None
                block_hash_ = block_msg.prev_block_hash()
            else:
                break

    def iterate_recent_block_hashes(
            self,
            max_count: int = gateway_constants.TRACKED_BLOCK_MAX_HASH_LOOKUP) -> Iterator[Sha256Hash]:
        """
        :param max_count:
        :return: Iterator[Sha256Hash] in descending order (last -> first)
        """
        if self._highest_block_number not in self._block_hashes_by_height:
            return iter([])
        block_hash = self._block_hashes_by_height[self._highest_block_number]
        return self.iterate_block_hashes_starting_from_hash(block_hash, max_count=max_count)
