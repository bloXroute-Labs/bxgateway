from typing import Optional, Dict, List, Tuple

from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.utils.expiring_dict import ExpiringDict
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import log_messages
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.services.abstract_block_queuing_service import AbstractBlockQueuingService
from bxutils import logging

logger = logging.get_logger(__name__)


class BlockQueuingServiceManager:
    block_storage: ExpiringDict[Sha256Hash, Optional[AbstractBlockMessage]]
    blockchain_peer_to_block_queuing_service: Dict[AbstractGatewayBlockchainConnection, AbstractBlockQueuingService]
    designated_queuing_service: Optional[AbstractBlockQueuingService] = None

    def __init__(
        self,
        block_storage: ExpiringDict[Sha256Hash, Optional[AbstractBlockMessage]],
        blockchain_peer_to_block_queuing_service: Dict[AbstractGatewayBlockchainConnection, AbstractBlockQueuingService]
    ) -> None:
        self.block_storage = block_storage
        self.blockchain_peer_to_block_queuing_service = blockchain_peer_to_block_queuing_service

    def __iter__(self):
        for queuing_service in self.blockchain_peer_to_block_queuing_service.values():
            yield queuing_service

    def is_in_any_queuing_service(self, block_hash: Sha256Hash) -> bool:
        """
        :param block_hash:
        :return: if block hash is in any queuing service.
        """
        for queuing_service in self:
            if block_hash in queuing_service:
                return True
        return False

    def is_in_common_block_storage(self, block_hash: Sha256Hash) -> bool:
        """
        :param block_hash:
        :return: if block message is in common block storage for block hash
        """
        block = self.block_storage.contents.get(block_hash)
        if block is None:
            return False
        else:
            return True

    def add_block_queuing_service(
        self,
        connection: AbstractGatewayBlockchainConnection,
        block_queuing_service: AbstractBlockQueuingService
    ) -> None:
        self.blockchain_peer_to_block_queuing_service[connection] = block_queuing_service
        if self.designated_queuing_service is None:
            self.designated_queuing_service = block_queuing_service

    def remove_block_queuing_service(self, connection: AbstractGatewayBlockchainConnection) -> None:
        if connection in self.blockchain_peer_to_block_queuing_service:
            designated_queuing_service = self.designated_queuing_service
            if designated_queuing_service is not None and designated_queuing_service.connection == connection:
                self.designated_queuing_service = None
            del self.blockchain_peer_to_block_queuing_service[connection]

        for queuing_service in self.blockchain_peer_to_block_queuing_service.values():
            self.designated_queuing_service = queuing_service
            return
        return

    def get_block_queuing_service(
        self, connection: AbstractGatewayBlockchainConnection
    ) -> Optional[AbstractBlockQueuingService]:
        if connection in self.blockchain_peer_to_block_queuing_service:
            return self.blockchain_peer_to_block_queuing_service[connection]
        else:
            logger.error(log_messages.ATTEMPTED_FETCH_FOR_NONEXISTENT_QUEUING_SERVICE, connection)
            return None

    def get_designated_block_queuing_service(self) -> Optional[AbstractBlockQueuingService]:
        if self.designated_queuing_service is not None:
            return self.designated_queuing_service
        else:
            logger.error(log_messages.ATTEMPTED_FETCH_FOR_NONEXISTENT_QUEUING_SERVICE, "(designated)")
            return None

    def push(
        self,
        block_hash: Sha256Hash, block_message: Optional[AbstractBlockMessage] = None,
        waiting_for_recovery: bool = False,
        node_received_from: Optional[AbstractGatewayBlockchainConnection] = None
    ) -> None:
        for node_conn, block_queuing_service in self.blockchain_peer_to_block_queuing_service.items():
            if node_received_from and node_conn == node_received_from:
                continue
            block_queuing_service.push(block_hash, block_message, waiting_for_recovery)

    def store_block_data(self, block_hash: Sha256Hash, block_message: AbstractBlockMessage) -> None:
        if self.designated_queuing_service is not None:
            queuing_service = self.designated_queuing_service
            assert queuing_service is not None
            # Note: Stores to common block storage. Individual service used in order to call protocol specific method.
            queuing_service.store_block_data(block_hash, block_message)
        else:
            logger.error(log_messages.ATTEMPTED_FETCH_FOR_NONEXISTENT_QUEUING_SERVICE, "(designated)")

    def get_block_data(self, block_hash: Sha256Hash) -> Optional[AbstractBlockMessage]:
        if block_hash in self.block_storage:
            return self.block_storage[block_hash]
        else:
            return None

    def remove(self, block_hash: Sha256Hash) -> None:
        for queuing_service in self:
            if block_hash in queuing_service:
                queuing_service.remove(block_hash)
        if block_hash in self.block_storage:
            del self.block_storage[block_hash]

    def update_recovered_block(
        self,
        block_hash: Sha256Hash,
        block_message: AbstractBlockMessage,
        node_received_from: Optional[AbstractGatewayBlockchainConnection] = None
    ) -> None:
        self.store_block_data(block_hash, block_message)
        for node_conn, queuing_service in self.blockchain_peer_to_block_queuing_service.items():
            if node_received_from and node_conn == node_received_from:
                continue
            queuing_service.update_recovered_block(block_hash, block_message)

    def get_length_of_each_queuing_service_stats_format(self) -> str:
        queue_lengths_and_endpoints: List[Tuple[IpEndpoint, int]] = []
        for queuing_service in self:
            queue_lengths_and_endpoints.append((queuing_service.connection.endpoint, (len(queuing_service))))
        queue_lengths_str = ', '.join(
            [f"{str(ip_endpoint)}: {str(queue_length)}" for ip_endpoint, queue_length in queue_lengths_and_endpoints]
        )
        return f"[{queue_lengths_str}]"
