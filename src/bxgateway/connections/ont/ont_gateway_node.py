from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import NULL_ONT_BLOCK_HASH, OntObjectHash
from bxgateway import ont_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.connections.ont.ont_node_connection import OntNodeConnection
from bxgateway.connections.ont.ont_relay_connection import OntRelayConnection
from bxgateway.connections.ont.ont_remote_connection import OntRemoteConnection
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService
from bxgateway.services.ont.ont_block_processing_service import OntBlockProcessingService
from bxgateway.services.ont.ont_block_queuing_service import OntBlockQueuingService
from bxgateway.services.ont.ont_neutrality_service import OntNeutralityService
from bxgateway.services.ont.ont_normal_block_cleanup_service import OntNormalBlockCleanupService
from bxutils.services.node_ssl_service import NodeSSLService

import bxgateway.messages.ont.ont_message_converter_factory as block_msg_converter_factory
import bxgateway.messages.ont.ont_consensus_message_converter_factory as consensus_msg_converter_factory


class OntGatewayNode(AbstractGatewayNode):
    def __init__(self, opts, node_ssl_service: NodeSSLService):
        super(OntGatewayNode, self).__init__(opts, node_ssl_service, ont_constants.TRACKED_BLOCK_CLEANUP_INTERVAL_S)

        self.block_processing_service = OntBlockProcessingService(self)

        self.message_converter = block_msg_converter_factory.create_ont_message_converter(
            self.opts.blockchain_net_magic,
            self.opts
        )
        self.consensus_message_converter = consensus_msg_converter_factory.create_ont_consensus_message_converter(
            self.opts.blockchain_net_magic, self.opts
        )

        self.neutrality_service = OntNeutralityService(self)

        self.current_block_height = 0
        self.current_block_hash = NULL_ONT_BLOCK_HASH
        self.opts.has_fully_updated_tx_service = True

    def build_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        return OntNodeConnection(socket_connection, self)

    def build_relay_connection(self, socket_connection: AbstractSocketConnectionProtocol) -> AbstractRelayConnection:
        return OntRelayConnection(socket_connection, self)

    def build_remote_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        return OntRemoteConnection(socket_connection, self)

    def build_block_queuing_service(
        self,
        connection: AbstractGatewayBlockchainConnection
    ) -> AbstractBlockQueuingService:
        return OntBlockQueuingService(self, connection)

    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        if self.opts.use_extensions:
            from bxgateway.services.ont.ont_extension_block_cleanup_service import OntExtensionBlockCleanupService
            block_cleanup_service = OntExtensionBlockCleanupService(self, self.network_num)
        else:
            block_cleanup_service = OntNormalBlockCleanupService(self, self.network_num)
        return block_cleanup_service

    def update_current_block_height(self, new_block_height: int, new_block_hash: OntObjectHash):
        if new_block_height >= self.current_block_height:
            self.current_block_height = new_block_height
            self.current_block_hash = new_block_hash

