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
from bxgateway.services.btc.btc_normal_block_cleanup_service import BtcNormalBlockCleanupService
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService


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

    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        if self.opts.use_extensions:
            from bxgateway.services.btc.btc_extension_block_cleanup_service import BtcExtensionBlockCleanupService
            block_cleanup_service = BtcExtensionBlockCleanupService(self, self.network_num)
        else:
            block_cleanup_service = BtcNormalBlockCleanupService(self, self.network_num)
        return block_cleanup_service
