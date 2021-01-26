from typing import List, TYPE_CHECKING

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.models.broadcast_message_type import BroadcastMessageType
from bxcommon.utils import crypto
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import NULL_ONT_BLOCK_HASH, OntObjectHash
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats import stats_format
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon.utils.stats.stat_block_type import StatBlockType
from bxgateway import ont_constants, log_messages
from bxgateway.connections.ont.ont_base_connection_protocol import OntBaseConnectionProtocol
from bxgateway.messages.ont.consensus_ont_message import OntConsensusMessage
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.get_headers_ont_message import GetHeadersOntMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage, InventoryOntType
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.pong_ont_message import PongOntMessage
from bxgateway.messages.ont.ver_ack_ont_message import VerAckOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage
from bxgateway.utils.errors.message_conversion_error import MessageConversionError

if TYPE_CHECKING:
    from bxgateway.connections.ont.ont_node_connection import OntNodeConnection


class OntNodeConnectionProtocol(OntBaseConnectionProtocol):
    def __init__(self, connection: "OntNodeConnection") -> None:
        super(OntNodeConnectionProtocol, self).__init__(connection)

        connection.message_handlers.update({
            OntMessageType.VERSION: self.msg_version,
            OntMessageType.INVENTORY: self.msg_inv,
            OntMessageType.BLOCK: self.msg_block,
            OntMessageType.TRANSACTIONS: self.msg_tx,
            OntMessageType.GET_BLOCKS: self.msg_proxy_request,
            OntMessageType.GET_HEADERS: self.msg_get_headers,
            OntMessageType.GET_DATA: self.msg_get_data,
            OntMessageType.HEADERS: self.msg_headers,
            OntMessageType.CONSENSUS: self.msg_consensus
        })

        self.ping_interval_s = ont_constants.ONT_PING_INTERVAL_S

        # TODO: re-enable when  block cleanup polling is supported
        # if self.block_cleanup_poll_interval_s > 0:
        #     self.connection.node.alarm_queue.register_alarm(
        #         self.block_cleanup_poll_interval_s,
        #         self._request_blocks_confirmation
        #     )

    def msg_version(self, _msg: VersionOntMessage) -> None:
        self.connection.on_connection_established()
        reply = VerAckOntMessage(self.magic, self.is_consensus)
        self.connection.enqueue_msg(reply)

        self.connection.schedule_pings()

        if self.connection.is_active():
            self.node.on_blockchain_connection_ready(self.connection)

    def msg_inv(self, msg: InvOntMessage) -> None:
        if not self.node.should_process_block_hash():
            return

        contains_block = False
        inventory_requests = []
        block_hashes = []
        inventory_type, item_hashes = msg.inv_type()

        if inventory_type == InventoryOntType.MSG_BLOCK.value:
            for item_hash in item_hashes:
                block_hashes.append(item_hash)
                if item_hash not in self.node.blocks_seen.contents:
                    contains_block = True
                    inventory_requests.append(item_hash)
        else:
            for item_hash in item_hashes:
                inventory_requests.append(item_hash)

        # TODO: mark_blocks_and_request_cleanup

        for req in inventory_requests:
            get_data = GetDataOntMessage(
                magic=msg.magic(),
                inv_type=inventory_type,
                block=req
            )
            self.connection.enqueue_msg(get_data, prepend=contains_block)

        block_queuing_service = self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
        if block_queuing_service is not None:
            block_queuing_service.mark_blocks_seen_by_blockchain_node(block_hashes)

    def msg_get_data(self, msg: GetDataOntMessage) -> None:
        inventory_type, item_hash = msg.inv_type()
        if inventory_type == InventoryOntType.MSG_BLOCK.value:
            block_stats.add_block_event_by_block_hash(
                item_hash,
                BlockStatEventType.REMOTE_BLOCK_REQUESTED_BY_GATEWAY,
                network_num=self.connection.network_num,
                more_info="Protocol: {}, Network: {}".format(
                    self.node.opts.blockchain_protocol,
                    self.node.opts.blockchain_network
                )
            )
        block_queuing_service = self.node.block_queuing_service_manager.get_block_queuing_service(self.connection)
        if block_queuing_service is not None:
            block_queuing_service.send_block_to_node(item_hash)

    def msg_get_headers(self, msg: GetHeadersOntMessage):
        block_queuing_service = self.node.block_queueing_service_manager.get_block_queuing_service(self.connection)
        send_successful = False
        if block_queuing_service is not None:
            send_successful = block_queuing_service.try_send_header_to_node(msg.hash_stop())
        if not send_successful:
            self.msg_proxy_request(msg, self.connection)

    def msg_pong(self, msg: PongOntMessage):
        if msg.height() > self.node.current_block_height:
            reply = GetHeadersOntMessage(self.magic, 1, NULL_ONT_BLOCK_HASH, self.node.current_block_hash)
            self.connection.enqueue_msg(reply)

    def msg_headers(self, msg: HeadersOntMessage):
        if not self.node.should_process_block_hash():
            return

        header = msg.headers()[-1]
        raw_hash = crypto.double_sha256(header)
        reply = GetDataOntMessage(self.magic, InventoryOntType.MSG_BLOCK.value,
                                  OntObjectHash(buf=raw_hash, length=ont_constants.ONT_HASH_LEN))
        self.connection.enqueue_msg(reply)

    def msg_consensus(self, msg: OntConsensusMessage):
        if not self.node.opts.is_consensus:
            return
        if msg.consensus_data_type() != ont_constants.BLOCK_PROPOSAL_CONSENSUS_MESSAGE_TYPE:
            return

        block_hash = msg.block_hash()
        node = self.connection.node
        if not node.should_process_block_hash(block_hash):
            return

        node.block_cleanup_service.on_new_block_received(block_hash, msg.prev_block_hash())
        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE,
                                                  network_num=self.connection.network_num,
                                                  broadcast_type=BroadcastMessageType.CONSENSUS,
                                                  more_info="Protocol: {}, Network: {}".format(
                                                      node.opts.blockchain_protocol,
                                                      node.opts.blockchain_network
                                                  ),
                                                  msg_size=len(msg.rawbytes())
                                                  )

        if block_hash in self.connection.node.blocks_seen.contents:
            self.node.on_block_seen_by_blockchain_node(block_hash, self.connection)
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_RECEIVED_FROM_BLOCKCHAIN_NODE_IGNORE_SEEN,
                                                      network_num=self.connection.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS)
            self.connection.log_info(
                "Discarding duplicate consensus block {} from local blockchain node.",
                block_hash
            )
            return

        node.track_block_from_node_handling_started(block_hash)

        self.connection.log_info(
            "Processing consensus block {} from local blockchain node.",
            block_hash
        )

        # Broadcast BlockHoldingMessage through relays and gateways
        conns = self.node.broadcast(
            BlockHoldingMessage(block_hash, self.node.network_num),
            broadcasting_conn=self.connection,
            prepend_to_queue=True,
            connection_types=(ConnectionType.RELAY_BLOCK, ConnectionType.GATEWAY)
        )
        if len(conns) > 0:
            block_stats.add_block_event_by_block_hash(block_hash,
                                                      BlockStatEventType.BLOCK_HOLD_SENT_BY_GATEWAY_TO_PEERS,
                                                      network_num=self.node.network_num,
                                                      broadcast_type=BroadcastMessageType.CONSENSUS,
                                                      peers=conns
                                                      )

        try:
            bx_block, block_info = self.node.consensus_message_converter.block_to_bx_block(
                msg,
                self.node.get_tx_service(),
                self.node.opts.enable_block_compression,
                self.node.network.min_tx_age_seconds
            )
        except MessageConversionError as e:
            block_stats.add_block_event_by_block_hash(
                e.msg_hash,
                BlockStatEventType.BLOCK_CONVERSION_FAILED,
                network_num=self.connection.network_num,
                broadcast_type=BroadcastMessageType.CONSENSUS,
                conversion_type=e.conversion_type.value
            )
            self.connection.log_error(log_messages.BLOCK_COMPRESSION_FAIL_ONT_CONSENSUS, e.msg_hash, e)
            return

        if block_info.ignored_short_ids:
            self.connection.log_debug(
                "Ignoring {} new SIDs for {}: {}",
                len(block_info.ignored_short_ids), block_info.block_hash, block_info.ignored_short_ids
            )

        block_stats.add_block_event_by_block_hash(block_hash,
                                                  BlockStatEventType.BLOCK_COMPRESSED,
                                                  start_date_time=block_info.start_datetime,
                                                  end_date_time=block_info.end_datetime,
                                                  network_num=self.connection.network_num,
                                                  broadcast_type=BroadcastMessageType.CONSENSUS,
                                                  prev_block_hash=block_info.prev_block_hash,
                                                  original_size=block_info.original_size,
                                                  txs_count=block_info.txn_count,
                                                  blockchain_network=self.node.opts.blockchain_network,
                                                  blockchain_protocol=self.node.opts.blockchain_protocol,
                                                  matching_block_hash=block_info.compressed_block_hash,
                                                  matching_block_type=StatBlockType.COMPRESSED.value,
                                                  more_info="Consensus compression: {}->{} bytes, {}, {}; "
                                                            "Tx count: {}".format(
                                                      block_info.original_size,
                                                      block_info.compressed_size,
                                                      stats_format.percentage(block_info.compression_rate),
                                                      stats_format.duration(block_info.duration_ms),
                                                      block_info.txn_count
                                                  )
                                                  )

        self.node.block_processing_service._process_and_broadcast_compressed_block(
            bx_block, self.connection, block_info, block_hash
        )

    def _build_get_blocks_message_for_block_confirmation(self, hashes: List[Sha256Hash]) -> AbstractMessage:
        raise NotImplementedError
