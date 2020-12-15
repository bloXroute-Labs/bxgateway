from asyncio import Future
from typing import Optional, List, Tuple, Dict, Any, Union

from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.provider.abstract_ws_provider import AbstractWsProvider
from bxcommon.feed.feed_manager import FeedManager
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxutils import logging

logger = logging.get_logger(__name__)

SUBSCRIBE_REQUEST_ID = "1"


class MockEthWsProxyPublisher(AbstractWsProvider):

    def __init__(
        self,
        ws_uri: Optional[str],
        feed_manager: FeedManager,
        transaction_service: TransactionService,
        node: EthGatewayNode
    ) -> None:
        self.feed_manager = feed_manager
        self.transaction_service = transaction_service
        self.node = node
        # ok, lifecycle patterns are a bit different
        # pyre-fixme[6]: Expected `str` for 1st param but got `Optional[str]`.
        super().__init__(ws_uri, True)
        self.receiving_task: Optional[Future] = None

    def connect(self):
        pass

    async def call_rpc(
        self,
        method: str,
        params: Union[List[Any], Dict[Any, Any], None],
        request_id: Optional[str] = None
    ) -> JsonRpcResponse:
        return JsonRpcResponse(request_id=request_id, result={})

    async def revive(self) -> None:
        pass

    async def reconnect(self) -> None:
        pass

    async def subscribe(self, channel: str, options: Optional[Dict[str, Any]] = None) -> str:
        pass

    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        pass

    async def start(self) -> None:
        pass

    async def handle_notifications(self, subscription_id: str) -> None:
        pass

    def process_received_transaction(self, tx_hash: Sha256Hash) -> None:
        pass

    def process_transaction_with_contents(
        self, tx_hash: Sha256Hash, tx_contents: memoryview
    ) -> None:
        pass

    async def fetch_missing_transaction(self, tx_hash: Sha256Hash) -> None:
        pass

    def process_transaction_with_parsed_contents(
        self, tx_hash: Sha256Hash, parsed_tx: Optional[Dict[str, Any]]
    ) -> None:
        pass

    async def stop(self) -> None:
        pass
