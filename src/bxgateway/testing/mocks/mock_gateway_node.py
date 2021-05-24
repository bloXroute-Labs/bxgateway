# pyre-ignore-all-errors
from typing import Optional, List, Union

from mock import MagicMock

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.models.node_type import NodeType
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.services.transaction_service import TransactionService
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.btc.abstract_btc_block_cleanup_service import AbstractBtcBlockCleanupService
from bxgateway.services.btc.btc_block_queuing_service import BtcBlockQueuingService
from bxgateway.services.gateway_transaction_service import GatewayTransactionService
from bxgateway.services.push_block_queuing_service import PushBlockQueuingService
from bxgateway.testing.mocks.mock_blockchain_connection import MockMessageConverter
from bxutils.services.node_ssl_service import NodeSSLService


class _MockCleanupService(AbstractBtcBlockCleanupService):
    def __init__(self, node):
        super(_MockCleanupService, self).__init__(node, 1)

    def clean_block_transactions(self, block_msg: BlockBtcMessage, transaction_service: TransactionService) -> None:
        pass

    def contents_cleanup(self, block_msg: BlockBtcMessage, transaction_service: TransactionService) -> None:
        pass

    def is_marked_for_cleanup(self, block_hash: BtcObjectHash) -> bool:
        return False


class MockGatewayNode(AbstractGatewayNode):
    NODE_TYPE = NodeType.EXTERNAL_GATEWAY

    def __init__(self, opts, node_ssl_service: Optional[NodeSSLService] = None,
                 block_queueing_cls=BtcBlockQueuingService):
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        if node_ssl_service is None:
            node_ssl_service = MockNodeSSLService(self.NODE_TYPE, MagicMock())
        super(MockGatewayNode, self).__init__(opts, node_ssl_service)
        self.block_queuing_cls = block_queueing_cls

        self.broadcast_messages = []
        self.broadcast_to_nodes_messages = []
        self._tx_service = GatewayTransactionService(self, 0)
        self.block_cleanup_service = self._get_cleanup_service()
        self.message_converter = MockMessageConverter()
        if opts.use_extensions:
            from bxgateway.services.extension_gateway_transaction_service import ExtensionGatewayTransactionService
            self._tx_service = ExtensionGatewayTransactionService(self, self.network_num)
        else:
            self._tx_service = GatewayTransactionService(self, self.network_num)
        self.opts.has_fully_updated_tx_service = True
        self.requester = MagicMock()
        self.has_active_blockchain_peer = MagicMock(return_value=True)
        self.min_tx_from_node_gas_price = MagicMock()

    def broadcast(self, msg, broadcasting_conn=None, prepend_to_queue=False, connection_types=None):
        if connection_types is None:
            connection_types = [ConnectionType.RELAY_ALL]

        if len(connection_types) == 1 and connection_types[0] == ConnectionType.BLOCKCHAIN_NODE:
            self.broadcast_to_nodes_messages.append(msg)
        else:
            self.broadcast_messages.append((msg, connection_types))
        return [MockConnection(
            MockSocketConnection(1, self, ip_address="123.123.123.123", port=1000), self)]

    def get_tx_service(self, _network_num=None):
        return self._tx_service

    def build_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    def build_relay_connection(self, socket_connection: AbstractSocketConnectionProtocol) -> AbstractRelayConnection:
        pass

    def build_remote_blockchain_connection(
        self, socket_connection: AbstractSocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    def build_block_queuing_service(
        self,
        connection: AbstractGatewayBlockchainConnection
    ) -> PushBlockQueuingService:
        return self.block_queuing_cls(self, connection)

    def build_block_cleanup_service(self) -> AbstractBlockCleanupService:
        pass

    def set_known_total_difficulty(self, block_hash: Sha256Hash, total_difficulty: int) -> None:
        pass

    def log_txs_network_content(
        self, network_num: int, transaction_hash: Sha256Hash, transaction_contents: Union[bytearray, memoryview]
    ) -> None:
        pass

    def _get_cleanup_service(self) -> AbstractBtcBlockCleanupService:
        return _MockCleanupService(self)

    # Ethereum only method
    def on_transactions_in_block(self, transactions: List[Transaction]) -> None:
        pass

    def mock_add_blockchain_peer(
        self,
        connection: AbstractGatewayBlockchainConnection
    ) -> None:
        self.blockchain_peers.add(BlockchainPeerInfo(connection.peer_ip, connection.peer_port))
        block_queuing_service = self.build_block_queuing_service(connection)
        self.block_queuing_service_manager.add_block_queuing_service(connection, block_queuing_service)

    def get_private_key(self):
        return crypto_utils.make_private_key(helpers.generate_bytearray(111))

    def get_node_public_key(self, _ip,_port):
        return crypto_utils.private_to_public_key(self.get_private_key())

