from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Set, List, Optional, Iterator, cast, Tuple

from bxcommon import constants
from bxcommon.utils import memory_utils, crypto
from bxcommon.utils.alarm_queue import AlarmId
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.memory_utils import ObjectSize
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import hooks
from bxgateway import eth_constants, gateway_constants
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
from bxgateway.services.push_block_queuing_service import (
    PushBlockQueuingService,
)
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)


class EthBlockQueuingService(
    PushBlockQueuingService[
        InternalEthBlockInfo, BlockHeadersEthProtocolMessage
    ]
):
    block_checking_alarms: Dict[Sha256Hash, AlarmId]
    block_repeat_count: Dict[Sha256Hash, int]
    _block_parts: ExpiringDict[Sha256Hash, NewBlockParts]
    _block_hashes_by_height: ExpiringDict[int, Set[Sha256Hash]]
    _height_by_block_hash: ExpiringDict[Sha256Hash, int]
    _highest_block_number: int = 0

    def __init__(self, node: "AbstractGatewayNode"):
        super().__init__(node)
        self.node: "EthGatewayNode" = cast("EthGatewayNode", node)
        self.block_checking_alarms = {}
        self.block_repeat_count = defaultdict(int)
        self._block_parts = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "eth_block_queue_parts",
        )
        self._block_hashes_by_height = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "eth_block_queue_hashes_by_heights",
        )
        self._height_by_block_hash = ExpiringDict(
            node.alarm_queue,
            gateway_constants.MAX_BLOCK_CACHE_TIME_S,
            "eth_block_queue_height_by_hash"
        )

    def build_block_header_message(
        self, block_hash: Sha256Hash, block_message: InternalEthBlockInfo
    ) -> BlockHeadersEthProtocolMessage:
        if block_hash in self._block_parts:
            block_header_bytes = self._block_parts[
                block_hash
            ].block_header_bytes
        else:
            block_header_bytes = (
                block_message.to_new_block_parts().block_header_bytes
            )
        return BlockHeadersEthProtocolMessage.from_header_bytes(
            block_header_bytes
        )

    def send_block_to_node(
        self, block_hash: Sha256Hash, block_msg: Optional[InternalEthBlockInfo],
    ) -> None:
        assert block_msg is not None
        new_block_parts = self._block_parts[block_hash]

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

        self.node.log_blocks_network_content(self.node.network_num, block_msg)

    # pyre-fixme[14]: `get_previous_block_hash_from_message` overrides method defined in
    #  `PushBlockQueuingService` inconsistently.
    def get_previous_block_hash_from_message(
        self, block_message: NewBlockEthProtocolMessage
    ) -> Sha256Hash:
        return block_message.prev_block_hash()

    def get_block_parts(self, block_hash: Sha256Hash) -> Optional[NewBlockParts]:
        if block_hash not in self._block_parts:
            logger.debug("requested transaction info for a block not in the queueing service {}", block_hash)
            return None
        return self._block_parts[block_hash]

    def get_block_body_from_message(self, block_hash: Sha256Hash) -> Optional[BlockBodiesEthProtocolMessage]:
        block_parts = self.get_block_parts(block_hash)
        if block_parts is None:
            return None
        return BlockBodiesEthProtocolMessage.from_body_bytes(block_parts.block_body_bytes)

    # pyre-fixme[14]: `on_block_sent` overrides method defined in
    #  `PushBlockQueuingService` inconsistently.
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

    def push(
        self,
        block_hash: Sha256Hash,
        block_msg: Optional[InternalEthBlockInfo] = None,
        waiting_for_recovery: bool = False,
    ) -> None:
        if not waiting_for_recovery:
            assert block_msg is not None
            self._store_block_parts(block_hash, block_msg)
        super().push(block_hash, block_msg, waiting_for_recovery)

    def store_block_data(
        self,
        block_hash: Sha256Hash,
        block_msg: InternalEthBlockInfo
    ):
        super().store_block_data(block_hash, block_msg)
        self._store_block_parts(block_hash, block_msg)

    def mark_block_seen_by_blockchain_node(
        self,
        block_hash: Sha256Hash,
        block_message: Optional[InternalEthBlockInfo] = None,
    ) -> None:
        if block_message is not None:
            self._store_block_parts(block_hash, block_message)

        super().mark_block_seen_by_blockchain_node(block_hash, block_message)

        if block_hash in self.block_checking_alarms:
            self.node.alarm_queue.unregister_alarm(
                self.block_checking_alarms[block_hash]
            )
            del self.block_checking_alarms[block_hash]
            self.block_repeat_count.pop(block_hash, 0)

    def remove(self, block_hash: Sha256Hash) -> int:
        index = super().remove(block_hash)
        if block_hash in self._block_parts:
            del self._block_parts[block_hash]
            height = self._height_by_block_hash.contents.pop(block_hash, None)
            logger.trace("Removing block {} at height {}", block_hash, height)
            if height:
                self._block_hashes_by_height.contents.get(
                    height, set()
                ).discard(block_hash)
        return index

    def try_send_bodies_to_node(self, block_hashes: List[Sha256Hash]) -> bool:
        """
        Creates and sends block bodies to blockchain connection.
        """
        bodies = []
        for block_hash in block_hashes:
            if block_hash not in self._blocks:
                logger.debug("{} was not found in queue. Aborting.", block_hash)
                return False

            block_message = self._blocks[block_hash]
            if block_message is None:
                logger.debug("{} was not ready in the queue. Aborting", block_hash)
                return False

            if block_hash in self._block_parts:
                block_body_bytes = self._block_parts[block_hash].block_body_bytes
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
            logger.debug(
                "Appending {} body ({}) for sending to blockchain node.",
                block_hash,
                height
            )

        full_message = BlockBodiesEthProtocolMessage(None, bodies)
        self.node.send_msg_to_node(full_message)
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
                logger.debug("{} was not found in queue. Aborting.", block_hash)
                return False

            block_message = self._blocks[block_hash]
            if block_message is None:
                logger.debug("{} was not ready in the queue. Aborting", block_hash)
                return False

            partial_headers_message = self.build_block_header_message(
                block_hash, block_message
            )
            block_headers = partial_headers_message.get_block_headers()
            assert len(block_headers) == 1
            headers.append(block_headers[0])

            height = self._height_by_block_hash.contents.get(block_hash, None)
            logger.debug(
                "Appending {} header ({}) for sending to blockchain node.",
                block_hash,
                height
            )

        full_header_message = BlockHeadersEthProtocolMessage(None, headers)
        self.node.send_msg_to_node(full_header_message)
        return True

    def get_block_hashes_starting_from_hash(
        self, block_hash: Sha256Hash, max_count: int, skip: int, reverse: bool
    ) -> Tuple[bool, List[Sha256Hash]]:
        """
        Finds up to max_count block hashes in queue that we still have headers
        and block messages queued up for.

        Returns (success, [found_hashes])
        """
        if block_hash not in self._blocks:
            return False, []

        starting_height = self._height_by_block_hash[block_hash]
        logger.trace(
            "Found block {} had height {}. Continuing...",
            block_hash,
            starting_height,
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
            multiplier = -1
        else:
            multiplier = 1

        while (
            len(block_hashes) < max_count
            and height in self._block_hashes_by_height
        ):
            matching_hashes = self._block_hashes_by_height[height]

            # A fork has occurred: give up, and fallback to
            # remote blockchain sync
            if len(matching_hashes) > 1:
                logger.trace(
                    "Detected fork when searching for {} "
                    "block hashes starting from height {}.",
                    max_count,
                    block_height,
                )
                return False, []

            block_hashes.append(
                next(iter(self._block_hashes_by_height[height]))
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
            logger.trace(
                "Could not find all {} requested block hashes. Only got {}.",
                max_count,
                len(block_hashes),
            )

        return True, block_hashes

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
            if block_hash_ and block_hash_ in self._block_parts:
                yield block_hash_
                block_hash_ = self._block_parts[block_hash_].get_previous_block_hash()
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
        block_hashes = self._block_hashes_by_height[self._highest_block_number]
        block_hash = next(iter(block_hashes))
        if len(block_hashes) > 1:
            logger.debug(f"iterating over queued blocks starting for a possible fork {block_hash}")

        return self.iterate_block_hashes_starting_from_hash(block_hash, max_count=max_count)

    def get_block_height(self, block_hash: Sha256Hash) -> Optional[int]:
        block_height: Optional[int] = None
        if block_hash and block_hash in self._height_by_block_hash:
            block_height = self._height_by_block_hash[block_hash]
        return block_height

    def log_memory_stats(self):
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
            self.block_repeat_count,
            "block_queue_block_repeat_count",
            ObjectSize(
                size=len(self.block_repeat_count) * (crypto.SHA256_HASH_LEN + constants.UL_INT_SIZE_IN_BYTES),
                flat_size=0,
                is_actual_size=False
            ),
            object_item_count=len(self.block_repeat_count),
            object_type=memory_utils.ObjectType.BASE,
            size_type=memory_utils.SizeType.ESTIMATE
        )

    def _store_block_parts(
        self, block_hash: Sha256Hash, block_message: InternalEthBlockInfo
    ):
        new_block_parts = block_message.to_new_block_parts()
        self._block_parts[block_hash] = new_block_parts
        block_number = block_message.block_number()
        if block_number > 0:
            logger.trace(
                "Adding headers for block {} at height: {}",
                block_hash,
                block_number,
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
            del self.block_checking_alarms[block_hash]
            return constants.CANCEL_ALARMS
