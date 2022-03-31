import time
from collections import deque
from typing import List, Deque, cast, Union

from bxcommon.feed.eth.eth_new_transaction_feed import EthNewTransactionFeed
from bxcommon.feed.eth.eth_pending_transaction_feed import EthPendingTransactionFeed
from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxcommon.feed.feed import FeedKey
from bxcommon.feed.feed_source import FeedSource
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.eth import eth_common_utils, eth_common_constants
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import log_messages
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.new_block_parts import NewBlockParts
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import \
    BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_bodies_v66_eth_protocol_message import \
    BlockBodiesV66EthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import \
    BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_v66_eth_protocol_message import \
    BlockHeadersV66EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import \
    GetBlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_bodies_v66_eth_protocol_message import \
    GetBlockBodiesV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import \
    GetBlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_v66_eth_protocol_message import \
    GetBlockHeadersV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_node_data_eth_protocol_message import \
    GetNodeDataEthProtocolMessage
from bxgateway.messages.eth.protocol.get_node_data_v66_eth_protocol_message import \
    GetNodeDataV66EthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_eth_protocol_message import \
    GetReceiptsEthProtocolMessage
from bxgateway.messages.eth.protocol.get_receipts_v66_eth_protocol_message import \
    GetReceiptsV66EthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import \
    NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.new_block_hashes_eth_protocol_message import \
    NewBlockHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.new_pooled_transaction_hashes_eth_protocol_message import \
    NewPooledTransactionHashesEthProtocolMessage
from bxgateway.messages.eth.protocol.pooled_transactions_eth_protocol_message import \
    PooledTransactionsEthProtocolMessage
from bxgateway.messages.eth.protocol.pooled_transactions_v66_eth_protocol_message import \
    PooledTransactionsV66EthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message_v63 import \
    StatusEthProtocolMessageV63
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService
from bxgateway.services.gateway_transaction_service import ProcessTransactionMessageFromNodeResult
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import \
    gateway_bdn_performance_stats_service
from bxgateway.utils.stats.gateway_transaction_stats_service import \
    gateway_transaction_stats_service
from bxgateway.utils.stats.transaction_feed_stats_service import transaction_feed_stats_service
from bxutils import logging

logger = logging.get_logger(__name__)


class EthNodeConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, rlpx_cipher: RLPxCipher):
        super().__init__(connection, is_handshake_initiator, rlpx_cipher)
        #  NOTE - cannot call `msg_proxy_request` directly
        connection.message_handlers.update({
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.TRANSACTIONS: self.msg_tx,
            EthProtocolMessageType.NEW_POOLED_TRANSACTION_HASHES: self.msg_new_pooled_tx_hashes,
            EthProtocolMessageType.POOLED_TRANSACTIONS: self.msg_pooled_txs,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_get_block_headers,
            EthProtocolMessageType.GET_BLOCK_BODIES: self.msg_get_block_bodies,
            EthProtocolMessageType.GET_NODE_DATA: self.msg_get_node_data,
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

    def msg_new_pooled_tx_hashes(self, msg: NewPooledTransactionHashesEthProtocolMessage) -> None:
        transaction_hashes = msg.transaction_hashes()

        if not self.node.opts.has_fully_updated_tx_service:
            self.connection.log_debug(
                "Skipping {} announced transactions from Ethereum peer, not yet synced",
                len(transaction_hashes)
            )
            return

        new_tx_hashes = []
        for tx_hash in transaction_hashes:
            if not self.tx_service.has_transaction_contents_by_key(
                self.tx_service.get_transaction_key(tx_hash, None)
            ):
                new_tx_hashes.append(tx_hash)

        if new_tx_hashes:
            self.request_transactions(new_tx_hashes)
            self.connection.log_trace("Fetching {} announced transactions from Ethereum peer",
                                      len(new_tx_hashes))

    def msg_pooled_txs(
        self,
        msg: Union[PooledTransactionsV66EthProtocolMessage, PooledTransactionsEthProtocolMessage]
    ) -> None:

        if isinstance(msg, PooledTransactionsV66EthProtocolMessage):
            response = msg.get_message()
        else:
            response = msg

        # noinspection PyTypeChecker
        # pyre-ignore
        self.msg_tx(response)

    def msg_tx(self, msg: TransactionsEthProtocolMessage) -> None:
        if len(msg.rawbytes()) >= eth_common_constants.ETH_SKIP_TRANSACTIONS_SIZE:
            bytes_skipped_count = len(msg.rawbytes())
            self.connection.log_debug("Skipping {} bytes of transactions message.", bytes_skipped_count)
            gateway_transaction_stats_service.log_skipped_transaction_bytes(bytes_skipped_count)
        else:
            super().msg_tx(msg)

    def msg_tx_after_tx_service_process_complete(
        self, process_result: List[ProcessTransactionMessageFromNodeResult]
    ):
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
            self.request_block_headers(block_hash, 1, 0, 0)

        self.request_block_bodies([block_hash for block_hash, _ in block_hash_number_pairs])

    def request_block_bodies(self, block_hashes: List[Sha256Hash]) -> None:
        super().request_block_bodies(block_hashes)
        self._block_bodies_requests.append(block_hashes)

    def msg_get_block_headers(self, msg: Union[GetBlockHeadersEthProtocolMessage, GetBlockHeadersV66EthProtocolMessage]):
        if self._waiting_checkpoint_headers_request:
            super(EthNodeConnectionProtocol, self).msg_get_block_headers(msg)
        else:

            if isinstance(msg, GetBlockHeadersV66EthProtocolMessage):
                request_id = msg.get_request_id()
                headers_request = msg.get_message()
            else:
                request_id = None
                headers_request = msg

            block_queuing_service = cast(
                EthBlockQueuingService,
                self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
            )
            block_headers = self.node.block_processing_service.try_process_get_block_headers_request(
                headers_request,
                block_queuing_service
            )
            if block_headers is None:
                self.msg_proxy_request(msg, self.connection)
            else:
                self.send_block_headers(block_headers, request_id)

    def msg_get_block_bodies(self, msg: Union[GetBlockBodiesEthProtocolMessage, GetBlockBodiesV66EthProtocolMessage]):
        if isinstance(msg, GetBlockBodiesV66EthProtocolMessage):
            request_id = msg.get_request_id()
            bodies_request = msg.get_message()
        else:
            request_id = None
            bodies_request = msg

        block_queuing_service = cast(
            EthBlockQueuingService,
            self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
        )
        block_bodies = self.node.block_processing_service.try_process_get_block_bodies_request(
            bodies_request, block_queuing_service
        )

        if block_bodies is None:
            self.node.log_requested_remote_blocks(bodies_request.get_block_hashes())
            self.msg_proxy_request(bodies_request, self.connection)
        else:
            self.send_block_bodies(block_bodies, request_id)

    def msg_block_headers(self, msg: Union[BlockHeadersV66EthProtocolMessage, BlockHeadersEthProtocolMessage]):
        if not self.node.should_process_block_hash():
            return

        if isinstance(msg, BlockHeadersV66EthProtocolMessage):
            msg = msg.get_message()

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

    def msg_block_bodies(self, msg: Union[BlockBodiesV66EthProtocolMessage, BlockBodiesEthProtocolMessage]):
        if not self.node.should_process_block_hash():
            return

        if isinstance(msg, BlockBodiesV66EthProtocolMessage):
            msg = msg.get_message()

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

    def msg_get_receipts(
        self, msg: Union[GetReceiptsV66EthProtocolMessage, GetReceiptsEthProtocolMessage]
    ) -> None:
        if isinstance(msg, GetReceiptsV66EthProtocolMessage):
            request = msg.get_message()
        else:
            request = msg

        self.node.log_requested_remote_blocks(request.get_block_hashes())
        self.msg_proxy_request(msg, self.connection)

    def msg_get_node_data(
        self, msg: Union[GetNodeDataV66EthProtocolMessage, GetNodeDataEthProtocolMessage]
    ) -> None:
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
        raise NotImplementedError()

    def _build_get_blocks_message_for_block_confirmation(self, hashes: List[Sha256Hash]) -> AbstractMessage:
        raise NotImplementedError()

    def publish_transaction(
        self, tx_hash: Sha256Hash, tx_contents: memoryview, local_region: bool = True
    ) -> None:
        transaction_feed_stats_service.log_new_transaction(tx_hash)
        self.node.feed_manager.publish_to_feed(
            FeedKey(EthNewTransactionFeed.NAME, self.node.network_num),
            EthRawTransaction(
                tx_hash,
                tx_contents,
                FeedSource.BLOCKCHAIN_SOCKET,
                local_region=local_region
            )
        )
        transaction_feed_stats_service.log_pending_transaction_from_local(tx_hash)
        self.node.feed_manager.publish_to_feed(
            FeedKey(EthPendingTransactionFeed.NAME, network_num=self.node.network_num),
            EthRawTransaction(
                tx_hash,
                tx_contents,
                FeedSource.BLOCKCHAIN_SOCKET,
                local_region=local_region
            )
        )
