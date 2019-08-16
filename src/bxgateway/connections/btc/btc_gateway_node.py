from typing import Tuple

from bxcommon.network.socket_connection import SocketConnection
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection
from bxgateway.connections.btc.btc_remote_connection import BtcRemoteConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import InvBtcMessage, InventoryType
from bxgateway.services.block_queuing_service import BlockQueuingService
from bxgateway.services.btc.btc_block_processing_service import BtcBlockProcessingService
from bxgateway.services.btc.btc_block_queuing_service import BtcBlockQueuingService
from bxgateway.testing.btc_lossy_relay_connection import BtcLossyRelayConnection
from bxgateway.testing.test_modes import TestModes


class BtcGatewayNode(AbstractGatewayNode):
    def __init__(self, opts):
        super(BtcGatewayNode, self).__init__(opts)
        self.block_processing_service = BtcBlockProcessingService(self)

    def build_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                    from_me: bool) -> AbstractGatewayBlockchainConnection:
        return BtcNodeConnection(socket_connection, address, self, from_me)

    def build_relay_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                               from_me: bool) -> AbstractRelayConnection:
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            cls = BtcLossyRelayConnection
        else:
            cls = BtcRelayConnection

        relay_connection = cls(socket_connection, address, self, from_me)
        return relay_connection

    def build_remote_blockchain_connection(self, socket_connection: SocketConnection, address: Tuple[str, int],
                                           from_me: bool) -> AbstractGatewayBlockchainConnection:
        return BtcRemoteConnection(socket_connection, address, self, from_me)

    def build_block_queuing_service(self) -> BlockQueuingService:
        return BtcBlockQueuingService(self)

    def send_msg_to_node(self, msg):
        super(BtcGatewayNode, self).send_msg_to_node(msg)

        # After sending block message to Bitcoin node sending INV message for the same block to the node
        # This is needed to update Synced Headers value of the gateway peer on the Bitcoin node
        # If Synced Headers is not up-to-date than Bitcoin node does not push compact blocks to the gateway
        if isinstance(msg, BlockBtcMessage):
            inv_msg = InvBtcMessage(magic=msg.magic(), inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())])
            self.send_msg_to_node(inv_msg)
