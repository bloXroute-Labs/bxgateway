import time
from collections import deque
from typing import List, Deque, cast, Union

from bxcommon.feed.feed import FeedKey
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.eth import eth_common_utils, eth_common_constants
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash, NULL_SHA256_HASH
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.feed.feed_source import FeedSource
from bxgateway import log_messages
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.eth_exceptions import CipherNotInitializedError
from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import GetBlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_eth_protocol_message import GetReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import NewBlockHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message_v63 import StatusEthProtocolMessageV63
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.services.gateway_transaction_service import ProcessTransactionMessageFromNodeResult
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import \
    gateway_bdn_performance_stats_service
from bxgateway.utils.stats.gateway_transaction_stats_service import gateway_transaction_stats_service
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service
from bxutils import logging

logger = logging.get_logger(__name__)


class EthNodeConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, rlpx_cipher: RLPxCipher):
        super().__init__(connection, is_handshake_initiator, rlpx_cipher)

        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.TRANSACTIONS: self.msg_tx,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_get_block_headers,
            EthProtocolMessageType.GET_BLOCK_BODIES: self.msg_get_block_bodies,
            EthProtocolMessageType.GET_NODE_DATA: self.msg_proxy_request,
            EthProtocolMessageType.GET_RECEIPTS: self.msg_get_receipts,
            EthProtocolMessageType.BLOCK_HEADERS: self.msg_block_headers,
            EthProtocolMessageType.NEW_BLOCK: self.msg_block,
            EthProtocolMessageType.NEW_BLOCK_HASHES: self.msg_new_block_hashes,
            EthProtocolMessageType.BLOCK_BODIES: self.msg_block_bodies
        })

        self.waiting_checkpoint_headers_request = True

        # uses block hash as a key, and NewBlockParts structure as value
        self.pending_new_block_parts: ExpiringDict[
            Sha256Hash, NewBlockParts
        ] = ExpiringDict(
            self.node.alarm_queue,
            eth_common_constants.NEW_BLOCK_PARTS_MAX_WAIT_S,
            f"{str(self)}_eth_pending_block_parts"
        )

        # hashes of new blocks constructed from headers and bodies, ready to send to BDN
        self._ready_new_blocks: Deque[Sha256Hash] = deque()

        # queue of lists of hashes that are awaiting block bodies response
        self._block_bodies_requests: Deque[List[Sha256Hash]] = deque(
            maxlen=eth_common_constants.REQUESTED_NEW_BLOCK_BODIES_MAX_COUNT)

        if self.block_cleanup_poll_interval_s > 0:
            self.connection.node.alarm_queue.register_alarm(
                self.block_cleanup_poll_interval_s,
                self._request_blocks_confirmation
            )

        self.requested_blocks_for_confirmation: ExpiringDict[
            Sha256Hash, float
        ] = ExpiringDict(
            self.node.alarm_queue,
            eth_common_constants.BLOCK_CONFIRMATION_REQUEST_CACHE_INTERVAL_S,
            f"{str(self)}_eth_requested_blocks"
        )
        self._connection_established_time = 0.0

    def msg_status(self, msg: Union[StatusEthProtocolMessage, StatusEthProtocolMessageV63]):
        super(EthNodeConnectionProtocol, self).msg_status(msg)

        self.connection.on_connection_established()

        self.connection.schedule_pings()

        self.node.on_blockchain_connection_ready(self.connection)
        self.node.alarm_queue.register_alarm(
            eth_common_constants.CHECKPOINT_BLOCK_HEADERS_REQUEST_WAIT_TIME_S,
            self._stop_waiting_checkpoint_headers_request
        )

        self._connection_established_time = time.time()

    def msg_tx(self, msg: TransactionsEthProtocolMessage) -> None:
        if len(msg.rawbytes()) >= eth_common_constants.ETH_SKIP_TRANSACTIONS_SIZE:
            bytes_skipped_count = len(msg.rawbytes())
            self.connection.log_debug("Skipping {} bytes of transactions message.", bytes_skipped_count)
            gateway_transaction_stats_service.log_skipped_transaction_bytes(bytes_skipped_count)
        else:
            super().msg_tx(msg)

    def msg_tx_after_tx_service_process_complete(self, process_result: List[ProcessTransactionMessageFromNodeResult]):
        # calculate minimal tx gas price only if transaction validation is enabled
        if not self.node.opts.transaction_validation:
            return

        for result in process_result:
            fee = eth_common_utils.raw_tx_gas_price(memoryview(result.transaction_contents), 0)
            self.node.min_tx_from_node_gas_price.add(fee)

    # pyre-fixme[14]: `msg_block` overrides method defined in
    #  `AbstractBlockchainConnectionProtocol` inconsistently.
    def msg_block(self, msg: NewBlockEthProtocolMessage) -> None:
        if not self.node.should_process_block_hash(msg.block_hash()):
            return

        self.node.set_known_total_difficulty(msg.block_hash(), msg.get_chain_difficulty())

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(msg)
        self.process_msg_block(internal_new_block_msg, msg.number())

        if self.node.opts.filter_txs_factor > 0:
            self.node.on_transactions_in_block(msg.txns())

    def msg_new_block_hashes(self, msg: NewBlockHashesEthProtocolMessage):
        if not self.node.should_process_block_hash(msg.block_hash()):
            return

        block_hash_number_pairs = []
        for block_hash, block_number in msg.get_block_hash_number_pairs():
            block_stats.add_block_event_by_block_hash(
                block_hash,
                BlockStatEventType.BLOCK_ANNOUNCED_BY_BLOCKCHAIN_NODE,
                network_num=self.connection.network_num,
                more_info="Protocol: {}, Network: {}. {}".format(
                    self.node.opts.blockchain_protocol,
                    self.node.opts.blockchain_network,
                    msg.extra_stats_data()
                ),
                block_height=block_number,
            )
            gateway_bdn_performance_stats_service.log_block_message_from_blockchain_node(
                self.connection.endpoint,
                False
            )

            if block_hash in self.node.blocks_seen.contents:
                self.node.on_block_seen_by_blockchain_node(block_hash, self.connection, block_number=block_number)
                block_stats.add_block_event_by_block_hash(
                    block_hash,
                    BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                    network_num=self.connection.network_num,
                    block_height=block_number,
                )
                self.connection.log_info(
                    "Ignoring duplicate block {} from local blockchain node.",
                    block_hash
                )
                continue

            self.node.on_block_seen_by_blockchain_node(block_hash, self.connection, block_number=block_number)

            self.node.track_block_from_node_handling_started(block_hash)
            block_hash_number_pairs.append((block_hash, block_number))

            self.connection.log_info(
                "Fetching block {} from local Ethereum node.",
                block_hash
            )

        if not block_hash_number_pairs:
            return

        for block_hash, block_number in block_hash_number_pairs:
            # pyre-fixme[6]: Expected `memoryview` for 1st param but got `None`.
            self.pending_new_block_parts.add(block_hash, NewBlockParts(None, None, block_number))
            self.connection.enqueue_msg(GetBlockHeadersEthProtocolMessage(None, block_hash.binary, 1, 0, 0))

        self.request_block_body([block_hash for block_hash, _ in block_hash_number_pairs])

    def request_block_body(self, block_hashes: List[Sha256Hash]):
        block_request_message = GetBlockBodiesEthProtocolMessage(
            None,
            block_hashes=[bytes(blk.binary) for blk in block_hashes]
        )
        self.connection.enqueue_msg(block_request_message)
        self._block_bodies_requests.append(block_hashes)

    def msg_get_block_headers(self, msg: GetBlockHeadersEthProtocolMessage):
        if self._waiting_checkpoint_headers_request:
            super(EthNodeConnectionProtocol, self).msg_get_block_headers(msg)
        else:
            block_queuing_service = cast(
                EthBlockQueuingService,
                self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
            )
            processed = self.node.block_processing_service.try_process_get_block_headers_request(
                msg,
                block_queuing_service
            )
            if not processed:
                self.msg_proxy_request(msg, self.connection)

    def msg_get_block_bodies(self, msg: GetBlockBodiesEthProtocolMessage):
        block_queuing_service = cast(
            EthBlockQueuingService,
            self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
        )
        processed = self.node.block_processing_service.try_process_get_block_bodies_request(msg, block_queuing_service)

        if not processed:
            self.node.log_requested_remote_blocks(msg.get_block_hashes())
            self.msg_proxy_request(msg, self.connection)

    def msg_block_headers(self, msg: BlockHeadersEthProtocolMessage):
        if not self.node.should_process_block_hash():
            return

        block_headers = msg.get_block_headers()

        if self.pending_new_block_parts.contents and len(block_headers) == 1:
            header_bytes = msg.get_block_headers_bytes()[0]
            block_hash_bytes = eth_common_utils.keccak_hash(header_bytes)
            block_hash = Sha256Hash(block_hash_bytes)

            if block_hash in self.pending_new_block_parts.contents:
                logger.debug("Received block header for new block {}", convert.bytes_to_hex(block_hash.binary))
                self.pending_new_block_parts.contents[block_hash].block_header_bytes = header_bytes
                self._check_pending_new_block(block_hash)
                self._process_ready_new_blocks()
                return

        if len(block_headers) > 0:
            block_hashes = [blk.hash_object() for blk in block_headers]
            block_hashes.insert(0, Sha256Hash(block_headers[0].prev_hash))
            self.node.block_cleanup_service.mark_blocks_and_request_cleanup(block_hashes)

        latest_block_number = 0
        latest_block_difficulty = 0

        block_queuing_service = self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
        if block_queuing_service is not None:
            for block_header in block_headers:
                block_queuing_service.mark_block_seen_by_blockchain_node(
                    block_header.hash_object(), None, block_header.number
                )

                if block_header.number > latest_block_number:
                    latest_block_number = block_header.number
                    latest_block_difficulty = block_header.difficulty

        self.node.block_processing_service.set_last_confirmed_block_parameters(
            latest_block_number, latest_block_difficulty
        )

    def msg_block_bodies(self, msg: BlockBodiesEthProtocolMessage):
        if not self.node.should_process_block_hash():
            return

        if self._block_bodies_requests:
            requested_hashes = self._block_bodies_requests.popleft()

            bodies_bytes = msg.get_block_bodies_bytes()

            if len(requested_hashes) != len(bodies_bytes):
                logger.debug("Expected {} bodies in response but received {}. Proxy message to remote node.",
                             len(requested_hashes), len(bodies_bytes))
                self._block_bodies_requests.clear()
                return

            logger.debug("Processing expected block bodies messages for blocks [{}]",
                         ", ".join([convert.bytes_to_hex(block_hash.binary) for block_hash in requested_hashes]))

            for block_hash, block_body_bytes in zip(requested_hashes, bodies_bytes):
                if block_hash in self.pending_new_block_parts.contents:
                    logger.debug("Received block body for pending new block {}",
                                 convert.bytes_to_hex(block_hash.binary))
                    self.pending_new_block_parts.contents[block_hash].block_body_bytes = block_body_bytes
                    self._check_pending_new_block(block_hash)
                elif self.node.block_cleanup_service.is_marked_for_cleanup(block_hash):
                    transactions_hashes = \
                        BlockBodiesEthProtocolMessage.from_body_bytes(block_body_bytes).get_block_transaction_hashes(0)
                    # pyre-fixme[16]: `AbstractBlockCleanupService` has no attribute
                    #  `clean_block_transactions_by_block_components`.
                    self.node.block_cleanup_service.clean_block_transactions_by_block_components(
                        transaction_service=self.node.get_tx_service(),
                        block_hash=block_hash,
                        transactions_list=transactions_hashes
                    )
                else:
                    logger.warning(log_messages.REDUNDANT_BLOCK_BODY,
                                   convert.bytes_to_hex(block_hash.binary))

            self._process_ready_new_blocks()

    def msg_get_receipts(self, msg: GetReceiptsEthProtocolMessage) -> None:
        self.node.log_requested_remote_blocks(msg.get_block_hashes())
        self.msg_proxy_request(msg, self.connection)

    def _stop_waiting_checkpoint_headers_request(self):
        self._waiting_checkpoint_headers_request = False

    def _check_pending_new_block(self, block_hash):
        if block_hash in self.pending_new_block_parts.contents:
            pending_new_block = self.pending_new_block_parts.contents[block_hash]

            if pending_new_block.block_header_bytes is not None and pending_new_block.block_body_bytes is not None:
                self._ready_new_blocks.append(block_hash)

    def _process_ready_new_blocks(self):
        while self._ready_new_blocks:
            ready_block_hash = self._ready_new_blocks.pop()
            pending_new_block = self.pending_new_block_parts.contents[ready_block_hash]
            self.pending_new_block_parts.remove_item(ready_block_hash)

            if ready_block_hash in self.node.blocks_seen.contents:
                self.node.on_block_seen_by_blockchain_node(
                    ready_block_hash, self.connection, block_number=pending_new_block.block_number
                )
                self.connection.log_info(
                    "Discarding already seen block {} received in block bodies msg from local blockchain node.",
                    ready_block_hash
                )
                return

            last_known_total_difficulty = self.node.try_calculate_total_difficulty(
                ready_block_hash,
                pending_new_block
            )
            new_block_msg = InternalEthBlockInfo.from_new_block_parts(
                pending_new_block,
                last_known_total_difficulty
            )

            if self.is_valid_block_timestamp(new_block_msg):
                block_stats.add_block_event_by_block_hash(
                    ready_block_hash,
                    BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE,
                    network_num=self.connection.network_num,
                    more_info="Protocol: {}, Network: {}. {}".format(
                        self.node.opts.blockchain_protocol,
                        self.node.opts.blockchain_network,
                        new_block_msg.extra_stats_data()
                    ),
                    block_height=new_block_msg.block_number(),
                )
                gateway_bdn_performance_stats_service.log_block_from_blockchain_node(self.connection.endpoint)

                canceled_recovery = self.node.on_block_seen_by_blockchain_node(
                    ready_block_hash, self.connection, new_block_msg, pending_new_block.block_number
                )
                if canceled_recovery:
                    return

                self.node.block_queuing_service_manager.push(
                    ready_block_hash, new_block_msg, node_received_from=self.connection
                )
                self.node.block_processing_service.queue_block_for_processing(
                    new_block_msg, self.connection
                )

    def _request_blocks_confirmation(self):
        # TODO: remove unused method
        try:
            super(EthNodeConnectionProtocol, self)._request_blocks_confirmation()
        except CipherNotInitializedError:
            logger.info(
                "Failed to request block confirmation due to bad cipher state, "
                "probably because the handshake was not completed yet."
            )
        return self.block_cleanup_poll_interval_s

    def _build_get_blocks_message_for_block_confirmation(self, hashes: List[Sha256Hash]) -> AbstractMessage:
        # TODO: remove unused method
        block_hash = NULL_SHA256_HASH
        for block_hash in hashes:
            last_attempt = self.requested_blocks_for_confirmation.contents.get(block_hash, 0)
            if time() - last_attempt > \
                self.block_cleanup_poll_interval_s * eth_common_constants.BLOCK_CONFIRMATION_REQUEST_INTERVALS:
                break
        else:
            block_hash = hashes[0]

        self.requested_blocks_for_confirmation.add(block_hash, time.time())
        return GetBlockHeadersEthProtocolMessage(
            None,
            block_hash=bytes(block_hash.binary),
            amount=100,
            skip=0,
            reverse=0
        )

    def publish_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview, local_region: bool = True
    ) -> None:
        transaction_feed_stats_service.log_new_transaction(tx_hash)
        self.node.feed_manager.publish_to_feed(
            FeedKey(EthNewTransactionFeed.NAME),
            EthRawTransaction(
                tx_hash,
                tx_contents,
                FeedSource.BLOCKCHAIN_SOCKET,
                local_region=local_region
            )
        )
        transaction_feed_stats_service.log_pending_transaction_from_local(tx_hash)
        self.node.feed_manager.publish_to_feed(
            FeedKey(EthPendingTransactionFeed.NAME),
            EthRawTransaction(
                tx_hash,
                tx_contents,
                FeedSource.BLOCKCHAIN_SOCKET,
                local_region=local_region
            )
        )
