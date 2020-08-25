from typing import List, Union

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxgateway.connections.ont.ont_base_connection_protocol import OntBaseConnectionProtocol
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ver_ack_ont_message import VerAckOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage


class OntRemoteConnectionProtocol(OntBaseConnectionProtocol):

    def __init__(self, connection):
        super(OntRemoteConnectionProtocol, self).__init__(connection)

        connection.message_handlers.update({
            OntMessageType.VERSION: self.msg_version,
            OntMessageType.BLOCK: self.msg_block,
            OntMessageType.HEADERS: self.msg_proxy_response,
            OntMessageType.INVENTORY: self.msg_inv,
        })
        self.ping_interval_s = gateway_constants.BLOCKCHAIN_PING_INTERVAL_S

    def msg_version(self, _msg: VersionOntMessage):
        """
        Handle version message.
        Gateway initiates connection, so do not check for misbehavior. Record that we received the version message,
        send a verack, and synchronize chains if need be.
        """
        self.connection.on_connection_established()
        reply = VerAckOntMessage(self.magic, self.is_consensus)
        self.connection.enqueue_msg(reply)
        self.connection.node.alarm_queue.register_alarm(self.ping_interval_s,
                                                        self.connection.send_ping)

        if self.connection.is_active():
            self.connection.node.on_remote_blockchain_connection_ready(self.connection)

    def msg_block(self, msg: BlockOntMessage) -> None:
        block_stats.add_block_event_by_block_hash(
            msg.block_hash(),
            BlockStatEventType.REMOTE_BLOCK_RECEIVED_BY_GATEWAY,
            network_num=self.connection.network_num,
            more_info="Protocol: {}, Network: {}".format(
                self.connection.node.opts.blockchain_protocol,
                self.connection.node.opts.blockchain_network
            )
        )
        return self.msg_proxy_response(msg)

    def msg_inv(self, msg: InvOntMessage):
        # Ignoring INV messages from REMOTE NODE to make sure it does not send new blocks to Ontology node
        pass

    def _build_get_blocks_message_for_block_confirmation(
        self, hashes: List[Sha256Hash]
    ) -> AbstractMessage:
        pass

    def _set_transaction_contents(
        self, tx_hash: Sha256Hash, tx_content: Union[memoryview, bytearray]
    ) -> None:
        pass
