from typing import Union, List

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxgateway.connections.btc.btc_base_connection_protocol import BtcBaseConnectionProtocol
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.ver_ack_btc_message import VerAckBtcMessage


class BtcRemoteConnectionProtocol(BtcBaseConnectionProtocol):

    def __init__(self, connection):
        super(BtcRemoteConnectionProtocol, self).__init__(connection)

        connection.message_handlers.update({
            BtcMessageType.VERSION: self.msg_version,
            BtcMessageType.BLOCK: self.msg_block,
            BtcMessageType.HEADERS: self.msg_proxy_response,
            BtcMessageType.INVENTORY: self.msg_inv,
        })
        self.ping_interval_s: int = gateway_constants.BLOCKCHAIN_PING_INTERVAL_S

    def msg_version(self, _msg):
        """
        Handle version message.
        Gateway initiates connection, so do not check for misbehavior. Record that we received the version message,
        send a verack, and synchronize chains if need be.
        """
        self.connection.on_connection_established()
        reply = VerAckBtcMessage(self.magic)
        self.connection.enqueue_msg(reply)
        self.connection.node.alarm_queue.register_alarm(self.ping_interval_s,
                                                        self.connection.send_ping)

        if self.connection.is_active():
            self.connection.node.on_remote_blockchain_connection_ready(self.connection)

    def msg_block(self, msg: BlockBtcMessage) -> None:
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

    def msg_inv(self, msg):
        # Ignoring INV messages from REMOTE NODE to make sure it does not send new blocks to Bitcoin node
        pass

    def _build_get_blocks_message_for_block_confirmation(
        self, hashes: List[Sha256Hash]
    ) -> AbstractMessage:
        pass

    def _set_transaction_contents(
        self, tx_hash: Sha256Hash, tx_content: Union[memoryview, bytearray]
    ) -> None:
        pass
