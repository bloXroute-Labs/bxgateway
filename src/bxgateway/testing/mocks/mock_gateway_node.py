# pyre-ignore-all-errors
from bxcommon.test_utils import helpers

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.connections.node_type import NodeType
from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxcommon.services.transaction_service import TransactionService
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.services.btc.abstract_btc_block_cleanup_service import AbstractBtcBlockCleanupService
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash
from bxgateway.services.btc.btc_block_queuing_service import BtcBlockQueuingService
from bxgateway.testing.mocks.mock_blockchain_connection import MockMessageConverter


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
    NODE_TYPE = NodeType.GATEWAY

    def __init__(self, opts):
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        super(MockGatewayNode, self).__init__(opts)

        self.broadcast_messages = []
        self.send_to_node_messages = []
        self._tx_service = TransactionService(self, 0)
        self.block_cleanup_service = self._get_cleanup_service()
        self.block_queuing_service = BtcBlockQueuingService(self)
        self.message_converter = MockMessageConverter()
        if opts.use_extensions:
            from bxcommon.services.extension_transaction_service import ExtensionTransactionService
            self._tx_service = ExtensionTransactionService(self, self.network_num)
        else:
            self._tx_service = TransactionService(self, self.network_num)

    def broadcast(self, msg, broadcasting_conn=None, prepend_to_queue=False, connection_types=None):
        if connection_types is None:
            connection_types = [ConnectionType.RELAY_ALL]

        self.broadcast_messages.append((msg, connection_types))
        return []

    def send_msg_to_node(self, msg):
        self.send_to_node_messages.append(msg)

    def get_tx_service(self, _network_num=None):
        return self._tx_service

    def build_blockchain_connection(
            self, socket_connection: SocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    def build_relay_connection(self, socket_connection: SocketConnectionProtocol) -> AbstractRelayConnection:
        pass

    def build_remote_blockchain_connection(
            self, socket_connection: SocketConnectionProtocol
    ) -> AbstractGatewayBlockchainConnection:
        pass

    def _get_cleanup_service(self) -> AbstractBtcBlockCleanupService:
        return _MockCleanupService(self)
