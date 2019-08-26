from collections import deque
from typing import List, Deque

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import eth_constants
from bxgateway.connections.eth.eth_base_connection_protocol import EthBaseConnectionProtocol
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

        # uses block hash as a key, and NewBlockParts structure as value
        self._pending_new_blocks_parts: ExpiringDict[Sha256Hash, NewBlockParts] = \
            ExpiringDict(self.node.alarm_queue, eth_constants.NEW_BLOCK_PARTS_MAX_WAIT_S)

        # hashes of new blocks constructed from headers and bodies, ready to send to BDN
        self._ready_new_blocks: Deque[Sha256Hash] = deque()

        # queue of block headers requests
        self._new_block_headers_requests: Deque[Sha256Hash] = deque(maxlen=eth_constants.REQUESTED_NEW_BLOCK_HEADERS_MAX_COUNT)

        # queue of lists of hashes that are awaiting block bodies response
        self._new_block_bodies_requests: Deque[List[Sha256Hash]] = deque(maxlen=eth_constants.REQUESTED_NEW_BLOCK_BODIES_MAX_COUNT)

    def msg_status(self, _msg):
        logger.debug("Status message received.")

        self.connection.state |= ConnectionState.ESTABLISHED
        self.node.node_conn = self.connection

        self.connection.send_ping()

        self.node.alarm_queue.register_alarm(eth_constants.CHECKPOINT_BLOCK_HEADERS_REQUEST_WAIT_TIME_S,
                                             self._stop_waiting_checkpoint_headers_request)

    def msg_block(self, msg: NewBlockEthProtocolMessage):
        self.node.set_known_total_difficulty(msg.block_hash(), msg.chain_difficulty())

        internal_new_block_msg = InternalEthBlockInfo.from_new_block_msg(msg)
        super(EthNodeConnectionProtocol, self).msg_block(internal_new_block_msg)

    def msg_new_block_hashes(self, msg: NewBlockHashesEthProtocolMessage):
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

            if block_hash in self.connection.node.blocks_seen.contents:
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                                                          network_num=self.connection.network_num)
                continue

            self.connection.node.track_block_from_node_handling_started(block_hash)
            block_hash_number_pairs.append((block_hash, block_number))
            self.connection.node.on_block_seen_by_blockchain_node(block_hash)

        if not block_hash_number_pairs:
            return

        for block_hash, block_number in block_hash_number_pairs:
            self._pending_new_blocks_parts.add(block_hash, NewBlockParts(None, None, block_number))
            self.node.send_msg_to_node(
                GetBlockHeadersEthProtocolMessage(None, block_hash.binary, 1, 0, False)
            )
            self._new_block_headers_requests.append(block_hash)

        self.node.send_msg_to_node(
            GetBlockBodiesEthProtocolMessage(None, [block_hash.binary for block_hash, _ in block_hash_number_pairs]))
        self._new_block_bodies_requests.append([block_hash for block_hash, _ in block_hash_number_pairs])

    def msg_get_block_headers(self, msg: GetBlockHeadersEthProtocolMessage):
        if self._waiting_checkpoint_headers_request:
            super(EthNodeConnectionProtocol, self).msg_get_block_headers(msg)
        else:
            processed = self.node.block_processing_service.try_process_get_block_headers_request(msg)
            if not processed:
                self.msg_proxy_request(msg)

    def msg_get_block_bodies(self, msg: GetBlockBodiesEthProtocolMessage):
        processed = self.node.block_processing_service.try_process_get_block_bodies_request(msg)

        if not processed:
            self.node.log_requested_remote_blocks(msg.get_block_hashes())
            self.msg_proxy_request(msg)

    def msg_block_headers(self, msg: BlockHeadersEthProtocolMessage):
        if self._new_block_headers_requests and len(msg.get_block_headers_bytes()) == 1:
            header_bytes = msg.get_block_headers_bytes()[0]
            block_hash_bytes = crypto_utils.keccak_hash(header_bytes)
            block_hash = Sha256Hash(block_hash_bytes)

            if block_hash == self._new_block_headers_requests[0]:
                self._new_block_headers_requests.pop()
                if block_hash in self._pending_new_blocks_parts.contents:
                    self._pending_new_blocks_parts.contents[block_hash].block_header_bytes = header_bytes
                    self._check_pending_new_block(block_hash)
                    self._process_ready_new_blocks()
                return

        block_hashes = [block_header.hash() for block_header in msg.get_block_headers()]
        self.connection.node.block_queuing_service.mark_blocks_seen_by_blockchain_node(block_hashes)

    def msg_block_bodies(self, msg: BlockBodiesEthProtocolMessage):
        if self._new_block_bodies_requests:
            requested_hashes = self._new_block_bodies_requests.pop()

            bodies_bytes = msg.get_block_bodies_bytes()

            if len(requested_hashes) != len(bodies_bytes):
                logger.warn("Expected {} bodies in response but received {}. Proxy message to remote node.",
                            len(requested_hashes), len(bodies_bytes))
                self._new_block_bodies_requests.clear()
                return

            for block_hash, block_body_bytes in zip(requested_hashes, bodies_bytes):
                if block_hash in self._pending_new_blocks_parts.contents:
                    self._pending_new_blocks_parts.contents[block_hash].block_body_bytes = block_body_bytes
                    self._check_pending_new_block(block_hash)

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

                self.node.block_processing_service.queue_block_for_processing(new_block_msg, self.connection)
