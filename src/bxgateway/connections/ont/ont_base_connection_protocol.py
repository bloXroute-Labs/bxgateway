import typing
from abc import abstractmethod
from typing import List, Union

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import ont_constants
from bxgateway import gateway_constants
from bxgateway.connections.abstract_blockchain_connection_protocol import AbstractBlockchainConnectionProtocol
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.messages.ont.addr_ont_message import AddrOntMessage
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.get_addr_ont_message import GetAddrOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage, InventoryOntType
from bxgateway.messages.ont.ont_message_factory import ont_message_factory
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ping_ont_message import PingOntMessage
from bxgateway.messages.ont.pong_ont_message import PongOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage
from bxutils import logging

logger = logging.get_logger(__name__)


class OntBaseConnectionProtocol(AbstractBlockchainConnectionProtocol):
    def __init__(self, connection: AbstractGatewayBlockchainConnection):
        super(OntBaseConnectionProtocol, self).__init__(
            connection,
            block_cleanup_poll_interval_s=ont_constants.BLOCK_CLEANUP_NODE_BLOCK_LIST_POLL_INTERVAL_S
                                                        )
        self.node = typing.cast("bxgateway.connections.ont.ont_gateway_node.OntGatewayNode", connection.node)
        self.magic = self.node.opts.blockchain_net_magic
        self.version = self.node.opts.blockchain_version
        self.is_consensus = self.node.opts.is_consensus

        connection.hello_messages = ont_constants.ONT_HELLO_MESSAGES
        connection.header_size = ont_constants.ONT_HDR_COMMON_OFF
        connection.message_factory = ont_message_factory
        connection.message_handlers = {
            OntMessageType.PING: self.msg_ping,
            OntMessageType.PONG: self.msg_pong,
            OntMessageType.GET_ADDRESS: self.msg_getaddr
        }

        version_msg = VersionOntMessage(self.magic, self.version, self.node.opts.sync_port,
                                        self.node.opts.http_info_port, self.node.opts.consensus_port,
                                        ont_constants.STARTUP_CAP, self.node.opts.blockchain_nonce, 0,
                                        self.node.opts.relay, self.node.opts.is_consensus,
                                        ont_constants.STARTUP_SOFT_VERSION.encode("utf-8"),
                                        self.node.opts.blockchain_services)
        connection.enqueue_msg(version_msg)

    def msg_block(self, msg):
        block_hash = msg.block_hash()

        if not self.node.should_process_block_hash(block_hash):
            return

        # TODO: block_cleanup_service
        if self.node.block_cleanup_service.is_marked_for_cleanup(block_hash):
            self.connection.log_trace("Marked block for cleanup: {}", block_hash)
            self.node.block_cleanup_service.clean_block_transactions(
                transaction_service=self.node.get_tx_service(),
                block_msg=msg
            )
        else:
            super().msg_block(msg)

        # After receiving block message sending INV message for the same block to Bitcoin node
        # This is needed to update Synced Headers value of the gateway peer on the Bitcoin node
        # If Synced Headers is not up-to-date than Bitcoin node does not push compact blocks to the gateway
        inv_msg = InvOntMessage(magic=self.node.opts.blockchain_net_magic,
                                inv_type=InventoryOntType.MSG_BLOCK, blocks=[block_hash])
        self.node.send_msg_to_node(inv_msg)
        self.node.update_current_block_height(msg.height(), block_hash)

    def msg_ping(self, msg: PingOntMessage):
        reply = PongOntMessage(self.magic, msg.height())
        self.connection.enqueue_msg(reply)

    def msg_pong(self, msg: PongOntMessage):
        pass

    def msg_getaddr(self, _msg: GetAddrOntMessage):
        reply = AddrOntMessage(self.magic)
        self.connection.enqueue_msg(reply)

    @abstractmethod
    def _build_get_blocks_message_for_block_confirmation(self, hashes: List[Sha256Hash]) -> AbstractMessage:
        pass

    @abstractmethod
    def _set_transaction_contents(self, tx_hash: Sha256Hash, tx_content: Union[memoryview, bytearray]) -> None:
        """
        set the transaction contents in the connection transaction service.
        since some buffers needs to be copied while others should not, this handler was added.
        avoid calling transaction_service.set_transactions_contents directly from this class or its siblings.
        :param tx_hash: the transaction hash
        :param tx_content: the transaction contents buffer
        """
        pass
