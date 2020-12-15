import time
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Dict, Set, List, Optional, Iterator, cast, Tuple, NamedTuple, \
    Deque, Callable

from bxcommon import constants
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxcommon.utils import memory_utils, crypto
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.memory_utils import ObjectSize
from bxcommon.utils.object_hash import Sha256Hash, NULL_SHA256_HASH
from bxcommon.utils.stats import hooks
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.feed.feed_source import FeedSource
from bxgateway import gateway_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import (
    BlockBodiesEthProtocolMessage,
)
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
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService, \
    BlockQueueEntry
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)
INITIAL_BLOCK_HEIGHT = -1


class OrderedQueuedBlock(NamedTuple):
    block_hash: Sha256Hash
    timestamp: float
    block_number: Optional[int]


class EthBlockInfo(NamedTuple):
    block_number: int
    block_hash: Sha256Hash


class SentEthBlockInfo(NamedTuple):
    block_number: int
    block_hash: Sha256Hash
    timestamp: float


class EthBlockQueuingService(
    AbstractBlockQueuingService[
        InternalEthBlockInfo, BlockHeadersEthProtocolMessage
    ]
):
    """
    Queues, pushes blocks to the Ethereum node, and handles get headers/bodies requests.

    If there are missing blocks in the network this class will not function optimally.
    """
    ordered_block_queue: Deque[OrderedQueuedBlock]

    block_checking_alarms: Dict[Sha256Hash, AlarmId]
    block_check_repeat_count: Dict[Sha256Hash, int]

    accepted_block_hash_at_height: ExpiringDict[int, Sha256Hash]
    sent_block_at_height: ExpiringDict[int, Sha256Hash]

    # best block sent to the Ethereum node
    best_sent_block: SentEthBlockInfo
    # best block accepted by Ethereum node
    best_accepted_block: EthBlockInfo

    _block_hashes_by_height: ExpiringDict[int, Set[Sha256Hash]]
    _height_by_block_hash: ExpiringDict[Sha256Hash, int]
    _highest_block_number: int = 0
    _recovery_alarms_by_block_hash: Dict[Sha256Hash, AlarmId]
    _next_push_alarm_id: Optional[AlarmId] = None
    _partial_chainstate: Deque[EthBlockInfo]

    def __init__(
        self,
        node: "AbstractGatewayNode",
        connection: AbstractGatewayBlockchainConnection,
    ):
        super().__init__(node, connection)
        self.node: "EthGatewayNode" = cast("EthGatewayNode", node)

        self.ordered_block_queue = deque(maxlen=gateway_constants.BLOCK_QUEUE_LENGTH_LIMIT)
        self.block_checking_alarms = {}
        self.block_check_repeat_count = defaultdict(int)

        self.accepted_block_hash_at_height = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            f"eth_block_queue_accepted_block_by_height_{self.connection.endpoint}"
        )
        self.sent_block_at_height = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            f"eth_block_queue_sent_block_at_height_{self.connection.endpoint}"
        )
        self.best_sent_block = SentEthBlockInfo(INITIAL_BLOCK_HEIGHT, NULL_SHA256_HASH, 0)
        self.best_accepted_block = EthBlockInfo(INITIAL_BLOCK_HEIGHT, NULL_SHA256_HASH)

        self._block_hashes_by_height = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            f"eth_block_queue_hashes_by_heights_{self.connection.endpoint}",
        )
        self._height_by_block_hash = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            f"eth_block_queue_height_by_hash_{self.connection.endpoint}"
        )
        self._recovery_alarms_by_block_hash = {}
        self._partial_chainstate = deque()

    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: InternalEthBlockInfo
    ) -> BlockHeadersEthProtocolMessage:
        if block_hash in self.node.block_parts_storage:
            block_header_bytes = self.node.block_parts_storage[
                block_hash
            ].block_header_bytes
        else:
            block_header_bytes = (
                block_message.to_new_block_parts().block_header_bytes
            )
        return BlockHeadersEthProtocolMessage.from_header_bytes(
            block_header_bytes
        )

    def push(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[InternalEthBlockInfo] = None,
        waiting_for_recovery: bool = False,
    ) -> None:
        if block_msg is None and not waiting_for_recovery:
            raise ValueError(
                "Block message is required if not waiting for recovery of the block."
            )

        if block_hash in self._blocks:
            raise ValueError(f"Block with hash {block_hash} already exists in the queue.")

        if not self.can_add_block_to_queuing_service(block_hash):
            self.connection.log_debug(
                "Skipping adding {} to queue. Block already seen.", block_hash
            )
            return

        if waiting_for_recovery:
            self._add_to_queue(block_hash, waiting_for_recovery, block_msg)
            self.connection.log_debug(
                "Appended recovering block {} to the end of the queue (behind {} others).",
                block_hash,
                len(self.ordered_block_queue) - 1
            )
            self._schedule_recovery_timeout(block_hash)
            return

        assert block_msg is not None
        block_number = block_msg.block_number()
        if self._check_for_sent_or_queued_forked_block(block_hash, block_number):
            # TODO: this line needs testing
            self.store_block_data(block_hash, block_msg)
            return

        position = self._add_to_queue(block_hash, waiting_for_recovery, block_msg)
        self.connection.log_debug(
            "Queued up block {} for sending to the blockchain node. "
            "Block is behind {} others (total size: {}).",
            block_hash,
            position,
            len(self.ordered_block_queue)
        )
        if position == 0:
            self._schedule_alarm_for_next_item()

    def update_recovered_block(
        self, block_hash: Sha256Hash, block_msg: InternalEthBlockInfo
    ) -> None:
        if block_hash not in self._blocks or block_hash in self._blocks_seen_by_blockchain_node:
            return

        self.remove_from_queue(block_hash)
        timeout_alarm = self._recovery_alarms_by_block_hash.pop(block_hash)
        self.node.alarm_queue.unregister_alarm(timeout_alarm)

        self._blocks_waiting_for_recovery[block_hash] = False

        block_number = block_msg.block_number()
        if self._is_block_stale(block_number):
            self.connection.log_info(
                "Discarding block {} at height {} in queuing service. Block is stale.",
                block_hash,
                block_number
            )
            return

        if self._check_for_sent_or_queued_forked_block(block_hash, block_number):
            self.connection.log_debug(
                "Discarding recovered block in queuing service."
            )
            return

        position = self._ordered_insert(block_hash, block_number, time.time())
        self.connection.log_debug(
            "Recovered block {}. Inserting into queue behind {} blocks (total length: {})",
            block_hash,
            position,
            len(self.ordered_block_queue)
        )
        if position == 0:
            self._schedule_alarm_for_next_item()

    def store_block_data(
        self,
        block_hash: Sha256Hash,
        block_msg: InternalEthBlockInfo
    ) -> None:
        super().store_block_data(block_hash, block_msg)
        self._store_block_parts(block_hash, block_msg)

    def mark_blocks_seen_by_blockchain_node(
        self, block_hashes: List[Sha256Hash]
    ):
        """
        Unused by Ethereum. Requires block number to function correctly.
        """

    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        block_message: Optional[InternalEthBlockInfo],
        block_number: Optional[int] = None,
    ) -> None:
        """
        Stores information about the block and marks the block heights to
        track the Ethereum node's blockchain state.
        Either block message or block number must be provided.

        This function may be called multiple times with blocks at the same height,
        and each subsequent call will updated the currently known chain state.
        """
        if block_message is not None:
            self.store_block_data(block_hash, block_message)
            block_number = block_message.block_number()
        if block_number is None and block_hash in self._height_by_block_hash:
            block_number = self._height_by_block_hash[block_hash]

        assert block_number is not None

        super().mark_block_seen_by_blockchain_node(block_hash, block_message)
        self.accepted_block_hash_at_height[block_number] = block_hash
        best_height, _ = self.best_accepted_block
        if block_number >= best_height:
            self.best_accepted_block = EthBlockInfo(block_number, block_hash)
            if block_message or block_hash in self.node.block_parts_storage:
                self.node.publish_block(
                    block_number, block_hash, block_message, FeedSource.BLOCKCHAIN_SOCKET
                )

        if block_hash in self.block_checking_alarms:
            self.node.alarm_queue.unregister_alarm(
                self.block_checking_alarms[block_hash]
            )
            del self.block_checking_alarms[block_hash]
            self.block_check_repeat_count.pop(block_hash, 0)

        self.remove_from_queue(block_hash)
        self._schedule_alarm_for_next_item()

    def remove(self, block_hash: Sha256Hash) -> int:
        index = super().remove(block_hash)
        if block_hash in self._height_by_block_hash:
            height = self._height_by_block_hash.contents.pop(block_hash, None)
            self.connection.log_trace(
                "Removing block {} at height {} in queuing service",
                block_hash, height
            )
            if height:
                self._block_hashes_by_height.contents.get(
                    height, set()
                ).discard(block_hash)
        return index

    def remove_from_queue(self, block_hash: Sha256Hash) -> int:
        index = super().remove_from_queue(block_hash)
        for i in range(len(self.ordered_block_queue)):
            if self.ordered_block_queue[i].block_hash == block_hash:
                del self.ordered_block_queue[i]
                break
        return index

    def send_block_to_node(
        self, block_hash: Sha256Hash, block_msg: Optional[InternalEthBlockInfo] = None,
    ) -> None:
        assert block_msg is not None

        # block must always be greater than previous best
        block_number = block_msg.block_number()
        best_height, _best_hash, _ = self.best_sent_block
        assert block_number > best_height

        new_block_parts = self.node.block_parts_storage[block_hash]

        if block_msg.has_total_difficulty():
            new_block_msg = block_msg.to_new_block_msg()
            super(EthBlockQueuingService, self).send_block_to_node(
                block_hash, new_block_msg
            )
            self.node.set_known_total_difficulty(
                new_block_msg.block_hash(), new_block_msg.get_chain_difficulty()
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

        self.node.log_blocks_network_content(self.node.network_num, block_msg)
        self.sent_block_at_height[block_number] = block_hash
        self.best_sent_block = SentEthBlockInfo(block_number, block_hash, time.time())
        self._schedule_confirmation_check(block_hash)

        if self.node.opts.filter_txs_factor > 0:
            self.node.on_transactions_in_block(block_msg.to_new_block_msg().txns())

    def partial_chainstate(self, required_length: int) -> Deque[EthBlockInfo]:
        """
        Builds a current chainstate based on the current head. Attempts to maintain
        the minimal length required chainstate as required by the length from the
        head as specified.

        :param required_length: length to extend partial chainstate to if needed
        """
        best_sent_height, best_sent_hash, _ = self.best_sent_block
        if best_sent_height == INITIAL_BLOCK_HEIGHT:
            return deque()

        if len(self._partial_chainstate) == 0:
            self._partial_chainstate.append(EthBlockInfo(best_sent_height, best_sent_hash))

        chain_head_height, chain_head_hash = self._partial_chainstate[-1]
        if chain_head_hash != best_sent_hash:
            height = best_sent_height
            head_hash = best_sent_hash
            missing_entries = deque()
            while height > chain_head_height:

                try:
                    head = self.node.block_storage[head_hash]
                    if head is None:
                        break
                    missing_entries.appendleft(EthBlockInfo(height, head_hash))

                    head_hash = head.prev_block_hash()
                    height -= 1
                except KeyError:
                    break

            # append to partial chain state
            if head_hash == chain_head_height:
                self._partial_chainstate.extend(missing_entries)
            # reorganization is required, rebuild to expected length
            else:
                self._partial_chainstate = missing_entries

        tail_height, tail_hash = self._partial_chainstate[0]
        tail = self.node.block_storage[tail_hash]
        assert tail is not None

        while len(self._partial_chainstate) < required_length:
            try:
                tail_hash = tail.prev_block_hash()
                tail_height -= 1
                tail = self.node.block_storage[tail_hash]
                if tail is None:
                    break
                self._partial_chainstate.appendleft(EthBlockInfo(tail_height, tail_hash))
            except KeyError:
                break

        return self._partial_chainstate

    def try_send_bodies_to_node(self, block_hashes: List[Sha256Hash]) -> bool:
        """
        Creates and sends block bodies to blockchain connection.
        """
        bodies = []
        for block_hash in block_hashes:
            if block_hash not in self._blocks:
                self.connection.log_debug(
                    "{} was not found in queuing service. Aborting attempt to send bodies.", block_hash
                )
                return False

            if not self.node.block_queuing_service_manager.is_in_common_block_storage(block_hash):
                self.connection.log_debug(
                    "{} was not in the block storage. Aborting attempt to send bodies.",
                    block_hash
                )
                return False
            block_message = cast(InternalEthBlockInfo, self.node.block_storage[block_hash])

            if block_hash in self.node.block_parts_storage:
                block_body_bytes = self.node.block_parts_storage[block_hash].block_body_bytes
            else:
                block_body_bytes = (
                    block_message.to_new_block_parts().block_body_bytes
                )

            partial_message = BlockBodiesEthProtocolMessage.from_body_bytes(
                block_body_bytes
            )
            block_bodies = partial_message.get_blocks()
            assert len(block_bodies) == 1
            bodies.append(block_bodies[0])

            height = self._height_by_block_hash.contents.get(block_hash, None)
            self.connection.log_debug(
                "Appending {} body ({}) for sending to blockchain node.",
                block_hash,
                height
            )

        full_message = BlockBodiesEthProtocolMessage(None, bodies)

        self.connection.enqueue_msg(full_message)
        return True

    def try_send_headers_to_node(self, block_hashes: List[Sha256Hash]) -> bool:
        """
        Creates and sends a block headers message to blockchain connection.

        In most cases, this method should be called with block hashes that are confirmed to
        exist in the block queuing service, but contains checks for safety for otherwise,
        and aborts the function if any headers are not found.
        """
        headers = []
        for block_hash in block_hashes:
            if block_hash not in self._blocks:
                self.connection.log_debug(
                    "{} was not found in queuing service. Aborting attempt to send headers.",
                    block_hash
                )
                return False

            if not self.node.block_queuing_service_manager.is_in_common_block_storage(block_hash):
                self.connection.log_debug(
                    "{} was not in block storage. Aborting attempt to send headers",
                    block_hash
                )
                return False
            block_message = cast(InternalEthBlockInfo, self.node.block_storage[block_hash])

            partial_headers_message = self.build_block_header_message(
                block_hash, block_message
            )
            block_headers = partial_headers_message.get_block_headers()
            assert len(block_headers) == 1
            headers.append(block_headers[0])

            height = self._height_by_block_hash.contents.get(block_hash, None)
            self.connection.log_debug(
                "Appending {} header ({}) for sending to blockchain node.",
                block_hash,
                height
            )

        full_header_message = BlockHeadersEthProtocolMessage(None, headers)

        self.connection.enqueue_msg(full_header_message)
        return True

    def get_block_hashes_starting_from_hash(
        self, block_hash: Sha256Hash, max_count: int, skip: int, reverse: bool
    ) -> Tuple[bool, List[Sha256Hash]]:
        """
        Finds up to max_count block hashes in queue that we still have headers
        and block messages queued up for.

        Returns (success, [found_hashes])
        """
        if block_hash not in self._blocks or block_hash not in self._height_by_block_hash:
            return False, []

        if block_hash in self._blocks_waiting_for_recovery and self._blocks_waiting_for_recovery[block_hash]:
            return False, []

        if not self.node.block_queuing_service_manager.is_in_common_block_storage(block_hash):
            return False, []

        best_height, _, _ = self.best_sent_block
        starting_height = self._height_by_block_hash[block_hash]
        look_back_length = best_height - starting_height + 1
        partial_chainstate = self.partial_chainstate(look_back_length)

        if not any(block_hash == chain_hash for _, chain_hash in partial_chainstate):
            block_too_far_back = len(partial_chainstate) != look_back_length
            self.connection.log_trace(
                "Block {} is not included in the current chainstate. "
                "Returning empty set. Chainstate missing entries: {}",
                block_hash,
                block_too_far_back
            )
            return not block_too_far_back, []

        self.connection.log_trace(
            "Found block {} had height {} in queuing service. Continuing...",
            block_hash,
            starting_height
        )
        return self.get_block_hashes_starting_from_height(
            starting_height, max_count, skip, reverse
        )

    def get_block_hashes_starting_from_height(
        self, block_height: int, max_count: int, skip: int, reverse: bool,
    ) -> Tuple[bool, List[Sha256Hash]]:
        """
        Performs a 'best-effort' search for block hashes.

        Gives up if a fork is detected in the requested section of the chain.
        Gives up if a block is requested below the most recent block that's
        not tracked by this service.
        Returns as many blocks as possible if some of the blocks requested
        are in the future and have no been produced yet.

        The resulting list starts at `block_height`, and is ascending if
        reverse=False, and descending if reverse=True.

        Returns (success, [found_hashes])
        """
        block_hashes: List[Sha256Hash] = []
        height = block_height
        if reverse:
            lowest_requested_height = block_height - (max_count * skip)
            multiplier = -1
        else:
            lowest_requested_height = block_height
            multiplier = 1

        chain_state: Optional[Set[Sha256Hash]] = None

        while (
            len(block_hashes) < max_count
            and height in self._block_hashes_by_height
        ):
            matching_hashes = self._block_hashes_by_height[height]

            # A fork has occurred: give up, and fallback to
            # remote blockchain sync
            if len(matching_hashes) > 1:
                self.connection.log_trace(
                    "Detected fork when searching for {} "
                    "block hashes starting from height {} "
                    "in queuing service.",
                    max_count,
                    block_height
                )
                if chain_state is None:
                    best_height, _, _ = self.best_sent_block
                    partial_state = self.partial_chainstate(
                        best_height - lowest_requested_height + 1
                    )
                    chain_state = set(block_hash for _, block_hash in partial_state)

                for candidate_hash in matching_hashes:
                    if candidate_hash in chain_state:
                        block_hashes.append(candidate_hash)
                        break
                else:
                    self.connection.log_debug(
                        "Unexpectedly, none of the blocks at height {} were part "
                        "of the chainstate.",
                        block_height
                    )
                    return False, []
            else:
                block_hashes.append(
                    next(iter(matching_hashes))
                )
            height += (1 + skip) * multiplier

        # If a block is requested too far in the past, abort and fallback
        # to remote blockchain sync
        if (
            height < self._highest_block_number
            and height not in self._block_hashes_by_height
            and max_count != len(block_hashes)
        ):
            return False, []

        # Ok, Ethereum expects as many hashes as node contains.
        if max_count != len(block_hashes):
            self.connection.log_trace(
                "Could not find all {} requested block hashes in block queuing service. Only got {}.",
                max_count,
                len(block_hashes)
            )

        return True, block_hashes

    def iterate_block_hashes_starting_from_hash(
        self,
        block_hash: Sha256Hash,
        max_count: int = gateway_constants.TRACKED_BLOCK_MAX_HASH_LOOKUP
    ) -> Iterator[Sha256Hash]:
        """
        iterate over cached blocks headers in descending order
        :param block_hash: starting block hash
        :param max_count: max number of elements to return
        :return: Iterator of block hashes in descending order
        """
        block_hash_ = block_hash
        for _ in range(max_count):
            if block_hash_ and block_hash_ in self.node.block_parts_storage:
                yield block_hash_
                block_hash_ = self.node.block_parts_storage[block_hash_].get_previous_block_hash()
            else:
                break

    def iterate_recent_block_hashes(
        self,
        max_count: int = gateway_constants.TRACKED_BLOCK_MAX_HASH_LOOKUP
    ) -> Iterator[Sha256Hash]:
        """
        :param max_count:
        :return: Iterator[Sha256Hash] in descending order (last -> first)
        """
        if self._highest_block_number not in self._block_hashes_by_height:
            return iter([])
        block_hashes = self._block_hashes_by_height[self._highest_block_number]
        block_hash = next(iter(block_hashes))
        if len(block_hashes) > 1:
            logger.debug(f"iterating over queued blocks starting for a possible fork {block_hash}")

        return self.iterate_block_hashes_starting_from_hash(block_hash, max_count=max_count)

    def get_block_parts(self, block_hash: Sha256Hash) -> Optional[NewBlockParts]:
        if block_hash not in self.node.block_parts_storage:
            self.connection.log_debug(
                "requested transaction info for a block {} not in the queueing service",
                block_hash,
            )
            return None
        return self.node.block_parts_storage[block_hash]

    def get_block_body_from_message(self, block_hash: Sha256Hash) -> Optional[BlockBodiesEthProtocolMessage]:
        block_parts = self.get_block_parts(block_hash)
        if block_parts is None:
            return None
        return BlockBodiesEthProtocolMessage.from_body_bytes(block_parts.block_body_bytes)

    def log_memory_stats(self) -> None:
        hooks.add_obj_mem_stats(
            self.__class__.__name__,
            self.node.network_num,
            self.block_checking_alarms,
            "block_queue_block_checking_alarms",
            ObjectSize(
                size=len(self.block_checking_alarms) * (crypto.SHA256_HASH_LEN + constants.UL_INT_SIZE_IN_BYTES),
                flat_size=0,
                is_actual_size=False
            ),
            object_item_count=len(self.block_checking_alarms),
            object_type=memory_utils.ObjectType.BASE,
            size_type=memory_utils.SizeType.ESTIMATE
        )

        hooks.add_obj_mem_stats(
            self.__class__.__name__,
            self.node.network_num,
            self.block_check_repeat_count,
            "block_queue_block_repeat_count",
            ObjectSize(
                size=len(self.block_check_repeat_count) * (crypto.SHA256_HASH_LEN + constants.UL_INT_SIZE_IN_BYTES),
                flat_size=0,
                is_actual_size=False
            ),
            object_item_count=len(self.block_check_repeat_count),
            object_type=memory_utils.ObjectType.BASE,
            size_type=memory_utils.SizeType.ESTIMATE
        )

    def _store_block_parts(
        self, block_hash: Sha256Hash, block_message: InternalEthBlockInfo
    ) -> None:
        new_block_parts = block_message.to_new_block_parts()
        if block_hash not in self.node.block_parts_storage:
            self.node.block_parts_storage[block_hash] = new_block_parts
        block_number = block_message.block_number()
        if block_number > 0:
            self.connection.log_trace(
                "Adding headers for block {} at height: {} in queuing service",
                block_hash,
                block_number
            )
            if block_number in self._block_hashes_by_height:
                self._block_hashes_by_height[block_number].add(block_hash)
            else:
                self._block_hashes_by_height.add(block_number, {block_hash})
            self._height_by_block_hash[block_hash] = block_number
            if block_number > self._highest_block_number:
                self._highest_block_number = block_number
        else:
            logger.trace(
                "No block height could be parsed for block: {}", block_hash
            )

    def _schedule_confirmation_check(self, block_hash: Sha256Hash) -> None:
        self.block_checking_alarms[
            block_hash
        ] = self.node.alarm_queue.register_alarm(
            eth_common_constants.CHECK_BLOCK_RECEIPT_DELAY_S,
            self._check_for_block_on_repeat,
            block_hash,
        )

    def _check_for_block_on_repeat(self, block_hash: Sha256Hash) -> float:
        get_confirmation_message = GetBlockHeadersEthProtocolMessage(
            None, block_hash.binary, 1, 0, 0
        )
        self.connection.enqueue_msg(get_confirmation_message)

        if self.block_check_repeat_count[block_hash] < eth_common_constants.CHECK_BLOCK_RECEIPT_MAX_COUNT:
            self.block_check_repeat_count[block_hash] += 1
            return eth_common_constants.CHECK_BLOCK_RECEIPT_INTERVAL_S
        else:
            del self.block_check_repeat_count[block_hash]
            del self.block_checking_alarms[block_hash]
            return constants.CANCEL_ALARMS

    def _schedule_alarm_for_next_item(self) -> None:
        """
        Sends the next top block if immediately available, otherwise schedules
        an alarm to check back in. Cancels all other instances of this alarm.
        Cleans out all stale blocks at front of queue.
        """
        next_push_alarm_id = self._next_push_alarm_id
        if next_push_alarm_id is not None:
            self.node.alarm_queue.unregister_alarm(next_push_alarm_id)
            self._next_push_alarm_id = None

        if len(self.ordered_block_queue) == 0:
            return

        while self.ordered_block_queue:
            block_hash, queued_time, block_number = self.ordered_block_queue[0]
            waiting_recovery = self._blocks_waiting_for_recovery[block_hash]

            if waiting_recovery:
                return

            assert block_number is not None

            if self._is_block_stale(block_number):
                self.connection.log_info(
                    "Discarding block {} from queuing service at height {}. Block is stale.",
                    block_hash,
                    block_number
                )
                self.remove_from_queue(block_hash)
                continue

            block_msg = cast(InternalEthBlockInfo, self.node.block_storage[block_hash])
            assert block_msg is not None
            self._try_immediate_send(block_hash, block_number, block_msg)
            _best_height, _best_hash, sent_time = self.best_sent_block
            elapsed_time = time.time() - sent_time
            timeout = self.node.opts.max_block_interval_s - elapsed_time
            self._run_or_schedule_alarm(timeout, self._send_top_block_to_node)
            break

    def _schedule_recovery_timeout(self, block_hash: Sha256Hash) -> None:
        self._recovery_alarms_by_block_hash[
            block_hash
        ] = self.node.alarm_queue.register_alarm(
            gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME,
            self._on_block_recovery_timeout,
            block_hash
        )

    def _run_or_schedule_alarm(self, timeout: float, func: Callable) -> None:
        if timeout > 0:
            self._next_push_alarm_id = self.node.alarm_queue.register_alarm(
                timeout, func
            )
        elif not self.node.has_active_blockchain_peer():
            self.node.alarm_queue.register_alarm(
                gateway_constants.NODE_READINESS_FOR_BLOCKS_CHECK_INTERVAL_S,
                func,
            )
        else:
            func()

    def _send_top_block_to_node(self) -> None:
        if len(self.ordered_block_queue) == 0:
            return

        if not self.node.has_active_blockchain_peer():
            self._schedule_alarm_for_next_item()
            return

        block_hash, timestamp, _ = self.ordered_block_queue[0]
        waiting_recovery = self._blocks_waiting_for_recovery[block_hash]

        if waiting_recovery:
            self.connection.log_debug(
                "Unable to send block to node, requires recovery. "
                "Block hash {}.",
                block_hash
            )
            self._schedule_alarm_for_next_item()
            return

        block_msg = cast(InternalEthBlockInfo, self.node.block_storage[block_hash])
        self.remove_from_queue(block_hash)

        self.send_block_to_node(block_hash, block_msg)

        self._schedule_alarm_for_next_item()
        return

    def _on_block_recovery_timeout(self, block_hash: Sha256Hash) -> None:
        self.connection.log_debug(
            "Removing block {} from queue. Recovery period has timed out.",
            block_hash
        )
        self.remove_from_queue(block_hash)
        if block_hash in self._blocks and self.node.block_queuing_service_manager.get_block_data(block_hash) is None:
            self.remove(block_hash)

    def _check_for_sent_or_queued_forked_block(self, block_hash: Sha256Hash, block_number: int) -> bool:
        """
        Returns True if a block has already been queued or sent at the same height.
        """

        is_duplicate = False
        more_info = ""
        for queued_block_hash, timestamp in self._block_queue:
            if (
                not self._blocks_waiting_for_recovery[queued_block_hash]
                and queued_block_hash in self._height_by_block_hash
            ):
                if block_number == self._height_by_block_hash[queued_block_hash]:
                    self.connection.log_info(
                        "In queuing service, fork detected at height {}. Setting aside block {} in favor of {}.",
                        block_number,
                        block_hash,
                        queued_block_hash

                    )
                    is_duplicate = True
                    more_info = "already queued"

        if block_number in self.sent_block_at_height:
            self.connection.log_info(
                "In queuing service, fork detected at height {}. "
                "Setting aside block {} in favor of already sent {}.",
                block_number,
                block_hash,
                self.sent_block_at_height[block_number]
            )
            is_duplicate = True
            more_info = "already sent"

        if block_number in self.accepted_block_hash_at_height:
            self.connection.log_info(
                "In queuing service, fork detected at height {}. "
                "Setting aside block {} in favor of already accepted {}.",
                block_number,
                block_hash,
                self.accepted_block_hash_at_height[block_number]

            )
            is_duplicate = True
            more_info = "already accepted"

        if is_duplicate:
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_IGNORE_DUPLICATE_HEIGHT,
                self.node.network_num,
                more_info=more_info
            )

        return is_duplicate

    def _is_block_stale(self, block_number: int) -> bool:
        """
        Check the best sent block to ensure the current block isn't in the past.

        Don't need to check ordered queue, since best sent block is always < its smallest entry.
        """
        best_sent_height, _, _ = self.best_sent_block
        best_accepted_height, _ = self.best_accepted_block
        return block_number <= best_sent_height or block_number <= best_accepted_height

    def _add_to_queue(
        self,
        block_hash: Sha256Hash,
        waiting_for_recovery: bool,
        block_msg: Optional[InternalEthBlockInfo]
    ) -> int:
        timestamp = time.time()
        self._block_queue.append(BlockQueueEntry(block_hash, timestamp))
        self._blocks_waiting_for_recovery[block_hash] = waiting_for_recovery
        self._blocks.add(block_hash)

        if block_msg is not None:
            self.store_block_data(block_hash, block_msg)
            return self._ordered_insert(block_hash, block_msg.block_number(), timestamp)
        else:
            # blocks with no number go to the end of the queue
            self.ordered_block_queue.append(
                OrderedQueuedBlock(block_hash, timestamp, None)
            )
            return len(self.ordered_block_queue) - 1

    def _ordered_insert(
        self,
        block_hash: Sha256Hash,
        block_number: int,
        timestamp: float
    ) -> int:
        index = 0
        while index < len(self.ordered_block_queue):
            queued_block_number = self.ordered_block_queue[index].block_number
            if (
                queued_block_number is not None
                and block_number > queued_block_number
            ):
                index += 1
            else:
                break

        if index == len(self.ordered_block_queue):
            self.ordered_block_queue.append(
                OrderedQueuedBlock(block_hash, timestamp, block_number)
            )
            return index

        if block_number == self.ordered_block_queue[index]:
            raise ValueError(f"Cannot insert block with duplicate number {block_number}")

        self.ordered_block_queue.insert(
            index, OrderedQueuedBlock(block_hash, timestamp, block_number)
        )
        return index

    def _try_immediate_send(self, block_hash: Sha256Hash, block_number: int, block_msg: InternalEthBlockInfo) -> bool:
        best_sent_height, _, _ = self.best_sent_block
        best_height, _ = self.best_accepted_block
        if (
            (best_height == INITIAL_BLOCK_HEIGHT and best_sent_height == INITIAL_BLOCK_HEIGHT)
            or best_height + 1 == block_number
        ):
            self.connection.log_debug(
                "Immediately propagating block {} at height {}. Block is of the next expected height.",
                block_hash, block_number
            )
            self.send_block_to_node(block_hash, block_msg)
            self.remove_from_queue(block_hash)
            self.node.publish_block(
                block_number, block_hash, block_msg, FeedSource.BDN_SOCKET
            )
            return True
        return False
