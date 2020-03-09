from collections import deque
from typing import List, Deque, Union, Dict
from time import time

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import convert
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash, NULL_SHA256_HASH
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import eth_constants
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
from bxgateway.eth_exceptions import CipherNotInitializedError
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
from bxgateway.utils.eth import crypto_utils
from bxgateway.utils.eth.remote_header_request import RemoteHeaderRequest
from bxutils import logging
from bxgateway import eth_constants

logger = logging.get_logger(__name__)


class EthNodeConnectionProtocol(EthBaseConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthNodeConnectionProtocol, self).__init__(connection, is_handshake_initiator, private_key, public_key)

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
        self._pending_new_blocks_parts: ExpiringDict[Sha256Hash, NewBlockParts] = \
            ExpiringDict(self.node.alarm_queue, eth_constants.NEW_BLOCK_PARTS_MAX_WAIT_S)

        # hashes of new blocks constructed from headers and bodies, ready to send to BDN
        self._ready_new_blocks: Deque[Sha256Hash] = deque()

        # queue of lists of hashes that are awaiting block bodies response
        self._block_bodies_requests: Deque[List[Sha256Hash]] = deque(maxlen=eth_constants.REQUESTED_NEW_BLOCK_BODIES_MAX_COUNT)

        if self.block_cleanup_poll_interval_s > 0:
            self.connection.node.alarm_queue.register_alarm(
                self.block_cleanup_poll_interval_s,
                self._request_blocks_confirmation
            )

        if eth_constants.TRACKED_BLOCK_CLEANUP_INTERVAL_S > 0:
            self.connection.node.alarm_queue.register_alarm(
                eth_constants.TRACKED_BLOCK_CLEANUP_INTERVAL_S,
                self._tracked_block_cleanup
            )

        self.requested_blocks_for_confirmation: ExpiringDict[Sha256Hash, float] = \
            ExpiringDict(self.node.alarm_queue, eth_constants.BLOCK_CONFIRMATION_REQUEST_CACHE_INTERVAL_S)

    def msg_status(self, _msg):
        self.connection.on_connection_established()

        self.connection.send_ping()

        self.node.on_blockchain_connection_ready(self.connection)
        self.node.alarm_queue.register_alarm(eth_constants.CHECKPOINT_BLOCK_HEADERS_REQUEST_WAIT_TIME_S,
                                             self._stop_waiting_checkpoint_headers_request)

    def msg_block(self, msg: NewBlockEthProtocolMessage):
        if not self.node.should_process_block_hash(msg.block_hash()):
            return

        self.node.set_known_total_difficulty(msg.block_hash(), msg.chain_difficulty())

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(msg)
        super().msg_block(internal_new_block_msg)

    def msg_new_block_hashes(self, msg: NewBlockHashesEthProtocolMessage):
        if not self.node.should_process_block_hash(msg.block_hash()):
            return

        block_hash_number_pairs = []
        for block_hash, block_number in msg.get_block_hash_number_pairs():
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_ANNOUNCED_BY_BLOCKCHAIN_NODE,
                                                      network_num=self.connection.network_num,
                                                      more_info="Protocol: {}, Network: {}. {}".format(
                                                          self.node.opts.blockchain_protocol,
                                                          self.node.opts.blockchain_network,
                                                          msg.extra_stats_data()
                                                      ))

            if block_hash in self.node.blocks_seen.contents:
                self.node.on_block_seen_by_blockchain_node(block_hash)
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                                                          network_num=self.connection.network_num)
                self.connection.log_info(
                    "Ignoring duplicate block {} from local blockchain node.",
                    block_hash
                )
                continue

            self.node.track_block_from_node_handling_started(block_hash)
            block_hash_number_pairs.append((block_hash, block_number))
            self.node.on_block_seen_by_blockchain_node(block_hash)

            self.connection.log_info(
                "Fetching block {} from local Ethereum node.",
                block_hash
            )

        if not block_hash_number_pairs:
            return

        for block_hash, block_number in block_hash_number_pairs:
            self._pending_new_blocks_parts.add(block_hash, NewBlockParts(None, None, block_number))
            self.node.send_msg_to_node(
                GetBlockHeadersEthProtocolMessage(None, block_hash.binary, 1, 0, False)
            )

        self.request_block_body([block_hash for block_hash, _ in block_hash_number_pairs])

    def request_block_body(self, block_hashes: List[Sha256Hash]):
        block_request_message = GetBlockBodiesEthProtocolMessage(
            None,
            block_hashes=[bytes(blk.binary) for blk in block_hashes]
        )
        self.node.send_msg_to_node(block_request_message)
        self._block_bodies_requests.append(block_hashes)

    def msg_get_block_headers(self, msg: GetBlockHeadersEthProtocolMessage):
        if self._waiting_checkpoint_headers_request:
            super(EthNodeConnectionProtocol, self).msg_get_block_headers(msg)
        else:
            processed = self.node.block_processing_service.try_process_get_block_headers_request(msg)
            if not processed:
                self.node.requested_remote_headers_queue.append(RemoteHeaderRequest(get_msg=msg, attempts=0))
                self.msg_proxy_request(msg)

    def msg_get_block_bodies(self, msg: GetBlockBodiesEthProtocolMessage):
        processed = self.node.block_processing_service.try_process_get_block_bodies_request(msg)

        if not processed:
            self.node.log_requested_remote_blocks(msg.get_block_hashes())
            self.msg_proxy_request(msg)

    def msg_block_headers(self, msg: BlockHeadersEthProtocolMessage):
        if not self.node.should_process_block_hash():
            return

        block_headers = msg.get_block_headers()

        if self._pending_new_blocks_parts.contents and len(block_headers) == 1:
            header_bytes = msg.get_block_headers_bytes()[0]
            block_hash_bytes = crypto_utils.keccak_hash(header_bytes)
            block_hash = Sha256Hash(block_hash_bytes)

            if block_hash in self._pending_new_blocks_parts.contents:
                logger.debug("Received block header for new block {}", convert.bytes_to_hex(block_hash.binary))
                self._pending_new_blocks_parts.contents[block_hash].block_header_bytes = header_bytes
                self._check_pending_new_block(block_hash)
                self._process_ready_new_blocks()
                return

        if len(block_headers) > 0:
            block_hashes = [blk.hash_object() for blk in block_headers]
            block_hashes.insert(0, Sha256Hash(block_headers[0].prev_hash))
            self.node.block_cleanup_service.mark_blocks_and_request_cleanup(block_hashes)
            self.node.block_queuing_service.mark_blocks_seen_by_blockchain_node(block_hashes)

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
                if block_hash in self._pending_new_blocks_parts.contents:
                    logger.debug("Received block body for pending new block {}",
                                 convert.bytes_to_hex(block_hash.binary))
                    self._pending_new_blocks_parts.contents[block_hash].block_body_bytes = block_body_bytes
                    self._check_pending_new_block(block_hash)
                elif self.node.block_cleanup_service.is_marked_for_cleanup(block_hash):
                    transactions_hashes = \
                        BlockBodiesEthProtocolMessage.from_body_bytes(block_body_bytes).get_block_transaction_hashes(0)
                    self.node.block_cleanup_service.clean_block_transactions_by_block_components(
                        transaction_service=self.node.get_tx_service(),
                        block_hash=block_hash,
                        transactions_list=transactions_hashes
                    )
                else:
                    logger.warning(
                        "Block body for hash {} is not in the list of pending new blocks and not marked for cleanup.",
                        convert.bytes_to_hex(block_hash.binary))

            self._process_ready_new_blocks()

    def msg_get_receipts(self, msg: GetReceiptsEthProtocolMessage) -> None:
        self.node.log_requested_remote_blocks(msg.get_block_hashes())
        self.msg_proxy_request(msg)

    def _stop_waiting_checkpoint_headers_request(self):
        self._waiting_checkpoint_headers_request = False

    def _check_pending_new_block(self, block_hash):
        if block_hash in self._pending_new_blocks_parts.contents:
            pending_new_block = self._pending_new_blocks_parts.contents[block_hash]

            if pending_new_block.block_header_bytes is not None and pending_new_block.block_body_bytes is not None:
                self._ready_new_blocks.append(block_hash)

    def _process_ready_new_blocks(self):
        while self._ready_new_blocks:
            ready_block_hash = self._ready_new_blocks.pop()
            pending_new_block = self._pending_new_blocks_parts.contents[ready_block_hash]
            last_known_total_difficulty = self.node.try_calculate_total_difficulty(ready_block_hash,
                                                                                   pending_new_block)
            new_block_msg = InternalEthBlockInfo.from_new_block_parts(pending_new_block,
                                                                      last_known_total_difficulty)
            self._pending_new_blocks_parts.remove_item(ready_block_hash)

            if self.is_valid_block_timestamp(new_block_msg):
                block_stats.add_block_event_by_block_hash(ready_block_hash,
                                                          BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE,
                                                          network_num=self.connection.network_num,
                                                          more_info="Protocol: {}, Network: {}. {}".format(
                                                              self.node.opts.blockchain_protocol,
                                                              self.node.opts.blockchain_network,
                                                              new_block_msg.extra_stats_data()
                                                          ))
                self.node.block_queuing_service.mark_block_seen_by_blockchain_node(
                    ready_block_hash, new_block_msg
                )
                self.node.block_processing_service.queue_block_for_processing(new_block_msg, self.connection)

    def _set_transaction_contents(self, tx_hash: Sha256Hash, tx_content: Union[memoryview, bytearray]) -> None:
        _tx_content = tx_content if isinstance(tx_content, bytearray) else bytearray(tx_content)
        self.connection.node.get_tx_service().set_transaction_contents(tx_hash, _tx_content)

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
                    self.block_cleanup_poll_interval_s * eth_constants.BLOCK_CONFIRMATION_REQUEST_INTERVALS:
                break
        else:
            block_hash = hashes[0]

        self.requested_blocks_for_confirmation.add(block_hash, time())
        return GetBlockHeadersEthProtocolMessage(
                None,
                block_hash=bytes(block_hash.binary),
                amount=100,
                skip=0,
                reverse=0
            )

    def _tracked_block_cleanup(self):
        if not self.connection.is_alive():
            return None
        node = self.connection.node
        tx_service = node.get_tx_service()
        block_queuing_service = self.node.block_queuing_service
        tracked_blocks = tx_service.get_oldest_tracked_block(0)
        for depth, block_hash in enumerate(block_queuing_service.iterate_recent_block_hashes()):
            if depth > node.network.block_confirmations_count and block_hash in tracked_blocks:
                self.node.block_cleanup_service.block_cleanup_request(block_hash)
        return eth_constants.TRACKED_BLOCK_CLEANUP_INTERVAL_S
