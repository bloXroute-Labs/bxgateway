from typing import List, TYPE_CHECKING, Union

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxcommon import constants
from bxgateway import ont_constants
from bxgateway.connections.ont.ont_base_connection_protocol import OntBaseConnectionProtocol
from bxgateway.messages.ont.get_headers_ont_message import GetHeadersOntMessage
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage, InventoryOntType
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ping_ont_message import PingOntMessage
from bxgateway.messages.ont.ver_ack_ont_message import VerAckOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage
from bxgateway.utils.ont.ont_object_hash import NULL_ONT_BLOCK_HASH

if TYPE_CHECKING:
    from bxgateway.connections.ont.ont_node_connection import OntNodeConnection


class OntNodeConnectionProtocol(OntBaseConnectionProtocol):
    def __init__(self, connection: "OntNodeConnection"):
        super(OntNodeConnectionProtocol, self).__init__(connection)

        connection.message_handlers.update({
            OntMessageType.VERSION: self.msg_version,
            OntMessageType.INVENTORY: self.msg_inv,
            OntMessageType.BLOCK: self.msg_block,
            OntMessageType.TRANSACTIONS: self.msg_tx,
            OntMessageType.GET_BLOCKS: self.msg_proxy_request,
            OntMessageType.GET_HEADERS: self.msg_get_headers,
            OntMessageType.GET_DATA: self.msg_get_data
            # TODO: add consensus message handler
        })

        self.connection.node.alarm_queue.register_alarm(
            self.block_cleanup_poll_interval_s,
            self._request_blocks_confirmation
        )

    def msg_version(self, msg: VersionOntMessage) -> None:
        self.connection.on_connection_established()
        reply = VerAckOntMessage(self.magic, self.is_consensus)
        self.connection.enqueue_msg(reply)

        self.node.alarm_queue.register_alarm(
            ont_constants.ONT_PING_INTERVAL_S,
            self.send_ping
        )

        if self.connection.is_active():
            self.node.on_blockchain_connection_ready(self.connection)

    def msg_inv(self, msg: InvOntMessage) -> None:
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

        self.node.block_queuing_service.mark_blocks_seen_by_blockchain_node(block_hashes)

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
        self.node.block_queuing_service.send_block_to_node(item_hash)

    def msg_get_headers(self, msg: GetHeadersOntMessage):
        send_successful = self.node.block_queuing_service.send_header_to_node(msg.hash_stop())
        if not send_successful:
            self.msg_proxy_request(msg)

    def send_ping(self):
        ping_msg = PingOntMessage(magic=self.magic, height=self.node.current_block_height)
        if self.connection.is_alive():
            self.connection.enqueue_msg(ping_msg)
            return ont_constants.ONT_PING_INTERVAL_S
        return constants.CANCEL_ALARMS

    def _build_get_blocks_message_for_block_confirmation(self, hashes: List[Sha256Hash]) -> AbstractMessage:
        # TODO: this is temporary, still need to check all the hashes from the input
        return GetHeadersOntMessage(magic=self.magic, length=2, hash_start=hashes[0], hash_stop=NULL_ONT_BLOCK_HASH)

    def _set_transaction_contents(self, tx_hash: Sha256Hash, tx_content: Union[memoryview, bytearray]) -> None:
        # TODO: need to check transaction contents
        self.connection.node.get_tx_service().set_transaction_contents(tx_hash, tx_content)
